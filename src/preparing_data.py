from typing import Any
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
import numpy as np
import requests
from pandas import pivot_table
from pathlib import Path
from zipfile import ZipFile
import logging

from config import settings

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

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
                    logger.info("Zip Dataset was downloaded")
            else:
                logger.error("The download was terminated")
        else:
            logger.info('File was already downloaded')

        with ZipFile(full_path, "r") as zip:
            zip.extractall(input_folder)
            logging.info("All csv files was unziped")
        full_path.unlink()
    else:
        logging.info("All the csv files already there")

def load_df(
        path_to_rating_csv: str,
) -> dd.DataFrame:
    rating = dd.read_csv(path_to_rating_csv)
    rating = rating.drop(columns=[
        settings.data.column_names.timestamp,
    ])
    logger.info(rating.dtypes)
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
    data.to_csv(
        path,
        index=False,
    )


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
