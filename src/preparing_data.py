from typing import Any
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
import numpy as np

from config import settings


def load_df(
        path_to_movie_csv: str,
        path_to_rating_csv: str,
) -> dd.DataFrame:
    movie = dd.read_csv(path_to_movie_csv)
    rating = dd.read_csv(path_to_rating_csv)
    merge_df = movie.merge(rating, how="left", on="movieId")
    merge_df = merge_df.categorize(columns=["title"])
    return merge_df


def train_test_split_df(
        data: dd.DataFrame,
        test_size: float = settings.data.test_size,
        random_state: int = settings.data.random_state,
        shuffle: bool = settings.data.shuffle,
) -> Any:
    return train_test_split(
        data,
        test_size=test_size,
        random_state=np.random.RandomState(random_state),
        shuffle=shuffle,
    )


def get_user_movie_df(data):
    return data.pivot_table(
        index="userId",
        columns="title",
        values="rating",
    ).fillna(0)
