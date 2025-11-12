from typing import List
from dask import dataframe as dd
from surprise import (
    Reader,
    Dataset,
    SVD,
    Prediction,
)
from surprise.dataset import DatasetAutoFolds
from tqdm import tqdm

from src.ml_models.model_base import (
    MLModelBase,
)
from src.utils.config import settings
class MLMatrixFactorizationSVD(MLModelBase):
    model_name = "SVD"

    def __init__(self) -> None:
        self.model = SVD()

    @staticmethod
    def _load_from_df(
            data: dd.DataFrame,
    ) -> DatasetAutoFolds:
        reader = Reader(rating_scale=(0.5, 5))
        df = data.compute() if isinstance(data, dd.DataFrame) else data
        dataset = Dataset.load_from_df(df[[
            settings.data.column_names.userId,
            settings.data.column_names.movieId,
            settings.data.column_names.rating,
        ]], reader)
        return dataset

    def fit(
            self,
            data: dd.DataFrame,
    ) -> None:
        dataset = self._load_from_df(data).build_full_trainset()
        self.model.fit(dataset)

    def predict(
            self,
            user_id: int,
            movie_id: int,
    ) -> Prediction:
        predict = self.model.predict(
            uid=user_id,
            iid=movie_id,
        )
        return predict

    def getting_recommended_movies(
            self,
            user_id: int,
            movies_list: List[int] | None = None,
            top_k: int = 50,
    ) -> List[int]:
        if not movies_list:
            all_items = set(self.model.trainset.all_items())
            user_items = set([j for (j, _) in self.model.trainset.ur[self.model.trainset.to_inner_uid(user_id)]])
            movies_list = list(all_items - user_items)

        predictions = []
        for iid in tqdm(movies_list, desc=f"Рекомендации для пользователя {user_id}", unit="item"):
            est = self.model.predict(user_id, iid).est
            predictions.append((iid, est))

        predictions.sort(key=lambda x: x[1], reverse=True)
        return [iid for iid, _ in predictions[:top_k]]
