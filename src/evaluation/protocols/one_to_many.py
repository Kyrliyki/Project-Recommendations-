import random
from typing import List, Tuple, Optional
from tqdm import tqdm
import logging
import dask.dataframe as dd

from src.evaluation.protocols.base_protocol import BaseEvaluationProtocol
from src.utils.config import settings

logger = logging.getLogger(__name__)

class OnePositiveToManyNegativesProtocol(BaseEvaluationProtocol):
    def __init__(
            self,
            num_negatives: int = 99,
            n_users: Optional[int] = None,
            threshold: float = None,
    ):
        super().__init__(name="one_to_many")
        self.num_negatives = num_negatives
        self.n_users = n_users
        self.threshold = threshold or settings.metrics.threshold_for_binarize
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
            user_positives = positive_df[
                positive_df[self.user_col] == user
                ][self.item_col].tolist()

            if not user_positives:
                continue

            seen_items = set(
                train_df[train_df[self.user_col] == user][self.item_col]
            ) | set(
                val_df[val_df[self.user_col] == user][self.item_col]
            )

            negative_candidates = list(all_items - seen_items)

            if len(negative_candidates) < self.num_negatives:
                continue

            for pos_item in user_positives:
                negatives = self.rng.sample(
                    negative_candidates,
                    self.num_negatives
                )

                candidates = [pos_item] + negatives
                self.rng.shuffle(candidates)

                test_cases.append(
                    (user, candidates, [pos_item])
                )

        logger.info(f"Подготовлено {len(test_cases)} тест-кейсов")
        return test_cases
