from dask import dataframe as dd

from abstract_classes import MLModelBase


class MLMatrixFactorizationSVD(MLModelBase):
    def __init__(self):
        pass

    def fit(
            self,
            data: dd.DataFrame
    ):
        pass

    def getting_recommended_movies(
            self,
            user_id: int,
            expected_number_of_recommendations: int
    ) -> list[int]:
        pass