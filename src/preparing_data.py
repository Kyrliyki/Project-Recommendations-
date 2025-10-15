from typing import Any
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split

from config import settings


def load_df(
        path_to_rating_csv: str,
) -> dd.DataFrame:
    rating = dd.read_csv(path_to_rating_csv)
    print(rating.dtypes)
    return rating


def train_test_split_df(
        data: dd.DataFrame,
        test_size: float = settings.data.test_size,
        random_state: int = settings.data.random_state,
        shuffle: bool = settings.data.shuffle,
) -> Any:
    return train_test_split(
        data,
        test_size=test_size,
        random_state=random_state,
        shuffle=shuffle,
    )


def save_df_to_csv(
        data: dd.DataFrame,
        path: str,
) -> None:
    data.to_csv(path)


def get_user_movie_df(
        data: dd.DataFrame,
) -> dd.DataFrame:
    # print(data.npartitions)
    # for n_part in range(data.npartitions):
    #     part= data.get_partition(n_part)
    #     pivot_data = part.pivot_table(
    #         index="userId",
    #         columns="movieId",
    #         values="rating",
    #     ).fillna(0)
    #     pivot_data.compute().to_csv(
    #         f"{settings.data.csv_save_train_path}/{n_part}.part",
    #     )
    #     print(n_part)
    data = data.categorize(columns=[
        settings.data.column_names.movieId
    ])
    pivot_data = dd.pivot_table(
        df=data,
        index=settings.data.column_names.userId,
        columns=settings.data.column_names.movieId,
        values=settings.data.column_names.rating,
    ).fillna(0)
    # print(pivot_data.npartitions)

    return pivot_data
