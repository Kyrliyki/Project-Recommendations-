import numpy as np
from dask import dataframe as dd
import pandas as pd
from surprise import (
    Reader,
    Dataset,
    SVD,
    Prediction,
)
from surprise.dataset import DatasetAutoFolds

from ml_models.model_base import (
    MLModelBase,
)
from config import settings


class MLMatrixFactorizationSVD(MLModelBase):
    all_movies: np.ndarray

    def __init__(self) -> None:
        self.model = SVD()

    @staticmethod
    def _load_from_df(
            data: dd.DataFrame,
    ) -> DatasetAutoFolds:
        reader = Reader(rating_scale=(1, 5))
        dataset = Dataset.load_from_df(data[[
            settings.data.column_names.userId,
            settings.data.column_names.movieId,
            settings.data.column_names.rating,
        ]], reader)
        return dataset

    def fit(
            self,
            data: dd.DataFrame,
    ) -> None:
        self.all_movies = data[settings.data.column_names.movieId].unique()
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
            expected_number_of_recommendations: int,
    ) -> np.ndarray:
        predicted_rating = pd.DataFrame(columns=["iid", "est"])
        for movie_id in self.all_movies:
            predict = self.predict(user_id, movie_id)
            if predict.r_ui is None:
                predicted_rating = pd.concat([
                    predicted_rating,
                    pd.DataFrame([{
                        "iid": movie_id,
                        "est": predict.est,
                    }]),
                ], ignore_index=True)
        predicted_rating = predicted_rating.sort_values(by="est", ascending=False)
        result = np.asarray(
            predicted_rating["iid"].head(expected_number_of_recommendations)
        )
        return result
