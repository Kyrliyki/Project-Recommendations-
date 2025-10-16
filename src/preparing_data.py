from typing import Any
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
import numpy as np
import requests
from pandas import pivot_table
from pathlib import Path
from zipfile import ZipFile

from config import settings

def download_csv(
        input_folder_path:str, 
        url: str):
    
    input_folder = Path(input_folder_path)
    input_folder.mkdir(exist_ok=True, parents=True)
    zip_file = "dataset.zip"
    full_path = input_folder / zip_file
    
    if len(list(input_folder.glob("*.csv"))) < 6:

        if not full_path.is_file():
            resp = requests.get(url)

            if resp.status_code == 200:
                with open(full_path, "wb") as file:
                    file.write(resp.content)
                    print("Zip Dataset was downloaded")
            else:
                print("Downloaded was not complete")
        else:
            print('File was already downloaded')

        with ZipFile(full_path, "r") as zip:
            zip.extractall(input_folder)
            print("All csv files was unziped")
        full_path.unlink()

def load_df(
        path_to_movie_csv: str,
        path_to_rating_csv: str,
) -> dd.DataFrame:
    movie = dd.read_csv(path_to_movie_csv)
    rating = dd.read_csv(path_to_rating_csv)
    merge_df = movie.merge(rating, how="left", on="movieId")
    merge_df = merge_df.categorize(columns=["title"])
    print(merge_df.dtypes)
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
        random_state=random_state,
        shuffle=shuffle,
    )


def get_user_movie_df(
        data: dd.DataFrame,
) -> dd.DataFrame:
    pivot_data = data.pivot_table(
        index="userId",
        columns="title",
        values="rating",
    ).fillna(0)
    return pivot_data
