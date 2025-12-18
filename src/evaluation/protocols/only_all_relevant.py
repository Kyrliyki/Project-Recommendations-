from typing import List, Tuple, Set, Optional
from tqdm import tqdm
import logging
import dask.dataframe as dd

from src.evaluation.protocols.base_protocol import BaseEvaluationProtocol

logger = logging.getLogger(__name__)

class OnlyAllPositives(BaseEvaluationProtocol):


    def __init__(
            self,
            min_relevant_items: Optional[int] = None,
            n_users: Optional[int] = None,
            threshold: float = None,
            sort_ascending: bool = True
    ):
        super().__init__(name="only_all_relevant")
        self.min_relevant_items = min_relevant_items
        self.n_users = n_users
        self.threshold = threshold
        self.sort_ascending = sort_ascending
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

        relevant_df = val_df[val_df[self.rating_col] >= self.threshold]

        if relevant_df.empty:
            raise ValueError("В validation нет релевантных оценок")

        user_counts = (
            relevant_df[self.user_col]
            .value_counts(ascending=self.sort_ascending)
        )

        user_counts = user_counts[user_counts >= self.min_relevant_items]

        if user_counts.empty:
            raise ValueError(
                f"Нет пользователей с ≥{self.min_relevant_items} релевантными айтемами"
            )

        if self.n_users:
            user_counts = user_counts.head(self.n_users)


        users = user_counts.index.tolist()

        logger.info(
            f"Выбрано {len(users)} пользователей "
            f"(релевантных айтемов от {user_counts.min()} до {user_counts.max()})"
        )

        test_cases = []

        for user in tqdm(users, desc="Подготовка пользователей", unit="user"):
            user_val = val_df[val_df[self.user_col] == user]

            all_items = user_val[self.item_col].unique().tolist()
            relevant_items = relevant_df[
                relevant_df[self.user_col] == user
                ][self.item_col].unique().tolist()

            if not relevant_items:
                continue

            test_cases.append(
                (user, all_items, relevant_items)
            )

        logger.info(f"Подготовлено {len(test_cases)} тест-кейсов")
        return test_cases

    def collect_rating_predictions(
            self,
            model_pipeline,
            test_cases: List[Tuple[int, List[int], List[int]]],
            validation: dd.DataFrame
    ) -> Tuple[List[List[float]], List[List[float]]]:
        logger.info("Сбор предсказаний рейтингов")

        val_df = validation.compute()

        all_y_true = []
        all_y_pred = []

        for user_id, _, _ in tqdm(test_cases, desc="Предсказания", unit="user"):
            user_df = val_df[val_df[self.user_col] == user_id]

            if user_df.empty:
                continue

            y_true_user = []
            y_pred_user = []

            for _, row in user_df.iterrows():
                true_rating = row[self.rating_col]
                item_id = row[self.item_col]

                if hasattr(model_pipeline, "predict_rating"):
                    pred = model_pipeline.predict_rating(user_id, item_id)
                elif hasattr(model_pipeline.model, "predict"):
                    pred = model_pipeline.model.predict(user_id, item_id).est
                else:
                    continue

                y_true_user.append(true_rating)
                y_pred_user.append(pred)

            if y_true_user:
                all_y_true.append(y_true_user)
                all_y_pred.append(y_pred_user)

        logger.info(f"Собрано предсказаний для {len(all_y_true)} пользователей")
        return all_y_true, all_y_pred