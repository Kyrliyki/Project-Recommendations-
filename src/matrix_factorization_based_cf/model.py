from dask import dataframe as dd
from surprise import (
    Reader,
    Dataset,
    SVD,
    accuracy,
)
from surprise.dataset import DatasetAutoFolds

from schemes import (
    MLModelBase,
    MetricsScheme,
)
from config import settings


class MLMatrixFactorizationSVD(MLModelBase):
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
        dataset = self._load_from_df(data).build_full_trainset()
        self.model.fit(dataset)

    def getting_recommended_movies(
            self,
            user_id: int,
            expected_number_of_recommendations: int
    ) -> list[int]:
        pass

    def calculating_metrics(
            self,
            test_data: dd.DataFrame
    ) -> MetricsScheme:
        predictions = self.model.test(
            [row for index, row in test_data.iterrows()]
        )
        # rating_true = [y.r_ui for y in predictions]
        # rating_prediction = [y.est for y in predictions]
        rmse = accuracy.rmse(predictions)
        print(rmse)
        return MetricsScheme(
            RMSE=rmse,
            Precision= 0,
            Recall= 0,
            MAP= 0,
            NDCG= 0,
        )


# train = dd.read_csv(settings.data.csv_save_train_path)
# test = dd.read_csv(settings.data.csv_save_test_path)
# model = MLMatrixFactorizationSVD()
# model.fit(train)
#
# print(model.calculating_metrics(test))