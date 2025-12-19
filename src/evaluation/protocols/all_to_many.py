import random
from typing import List, Tuple, Set, Optional
from tqdm import tqdm
import logging
import dask.dataframe as dd

from src.evaluation.protocols.base_protocol import BaseEvaluationProtocol

logger = logging.getLogger(__name__)

class AllPositivesToManyNegativesProtocol(BaseEvaluationProtocol):
    def __init__(
            self,
            num_negatives_per_positive: int = 99,
            n_users: Optional[int] = None,
            threshold: float = None,
    ):
        super().__init__(name="all_to_many")
        self.num_negatives_per_positive = num_negatives_per_positive
        self.n_users = n_users
        self.threshold = threshold
        self.rating_col = "rating"
        self.user_col = "userId"
        self.item_col = "movieId"

    def prepare_test_cases(
            self,
            train: dd.DataFrame,
            validation: dd.DataFrame
    ) -> List[Tuple[int, List[int], List[int]]]:
        logger.info(f"Подготовка данных по протоколу '{self.name}'")

        val_df = validation.compute()
        train_df = train.compute()

        positive_df = val_df[val_df[self.rating_col] >= self.threshold]

        if positive_df.empty:
            raise ValueError("В validation нет релевантных оценок")

        eligible_users = set(positive_df[self.user_col])
        eligible_users &= set(train_df[self.user_col])

        if self.n_users:
            users = self.rng.sample(
                list(eligible_users),
                min(self.n_users, len(eligible_users))
            )
        else:
            users = list(eligible_users)

        all_items = set(train_df[self.item_col]) | set(val_df[self.item_col])

        test_cases = []

        for user in tqdm(users, desc="Подготовка пользователей"):
            relevant_items = positive_df[
                positive_df[self.user_col] == user
                ][self.item_col].tolist()

            seen_items = set(
                train_df[train_df[self.user_col] == user][self.item_col]
            ) | set(
                val_df[val_df[self.user_col] == user][self.item_col]
            )

            negatives = list(all_items - seen_items)

            n_needed = len(relevant_items) * self.num_negatives_per_positive
            if len(negatives) < n_needed:
                continue

            sampled_negatives = self.rng.sample(negatives, n_needed)
            candidates = relevant_items + sampled_negatives
            self.rng.shuffle(candidates)

            test_cases.append((user, candidates, relevant_items))

        logger.info(f"Подготовлено {len(test_cases)} тест-кейсов")
        return test_cases

