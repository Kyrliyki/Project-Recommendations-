from typing import Any
import dask.dataframe as dd
import requests
from pathlib import Path
from zipfile import ZipFile
import logging
import pandas as pd
from src.utils.config import settings


logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

def download_csv(
        input_folder_path:str,
        url: str):
    """Загрузка датасета"""
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


def train_validation_test_split_ddf(
        data: dd.DataFrame,
        test_ratio: float = settings.data.test_size,
        validation_ratio: float = settings.data.validation_size
) -> Any:
    """Разделение Dask Dataframe на train, validation, test"""
    data_sorted = data.sort_values('timestamp')


    # Вычисляем граничные временные метки
    train_end_time = data_sorted['timestamp'].quantile(1 - test_ratio - validation_ratio).compute()
    val_end_time = data_sorted['timestamp'].quantile(1 - test_ratio).compute()

    # Разделяем по временным меткам
    train = data_sorted[data_sorted['timestamp'] <= train_end_time]
    validation = data_sorted[
        (data_sorted['timestamp'] > train_end_time) &
        (data_sorted['timestamp'] <= val_end_time)
        ]
    test = data_sorted[data_sorted['timestamp'] > val_end_time]


    return train, validation, test


def train_validation_test_split_ddf_on_users(
        data: dd.DataFrame,
        test_ratio: float = settings.data.test_size,
        validation_ratio: float = settings.data.validation_size
) -> Any:
    """Разделение по пользователям"""

    def split_user_group(user_group):
        """Обрабатывает всю группу пользователей за один вызов"""
        sorted_group = user_group.sort_values(['userId', 'timestamp'])

        sorted_group['user_interaction_num'] = sorted_group.groupby('userId').cumcount()

        # Считаем общее количество взаимодействий для каждого пользователя
        user_interaction_counts = sorted_group.groupby('userId').size()
        sorted_group = sorted_group.merge(
            user_interaction_counts.rename('user_total_interactions'),
            on='userId'
        )

        # Определяем границы для каждого пользователя
        sorted_group['train_end_idx'] = (sorted_group['user_total_interactions'] *
                                         (1 - test_ratio - validation_ratio)).astype(int)
        sorted_group['val_end_idx'] = (sorted_group['user_total_interactions'] *
                                       (1 - test_ratio)).astype(int)

        # Разделяем на train/validation/test
        train_mask = sorted_group['user_interaction_num'] < sorted_group['train_end_idx']
        val_mask = (sorted_group['user_interaction_num'] >= sorted_group['train_end_idx']) & \
                   (sorted_group['user_interaction_num'] < sorted_group['val_end_idx'])
        test_mask = sorted_group['user_interaction_num'] >= sorted_group['val_end_idx']

        train_data = sorted_group[train_mask].drop(columns=['user_interaction_num', 'user_total_interactions',
                                                            'train_end_idx', 'val_end_idx'])
        val_data = sorted_group[val_mask].drop(columns=['user_interaction_num', 'user_total_interactions',
                                                        'train_end_idx', 'val_end_idx'])
        test_data = sorted_group[test_mask].drop(columns=['user_interaction_num', 'user_total_interactions',
                                                          'train_end_idx', 'val_end_idx'])

        return train_data, val_data, test_data

    print("Начало разделения...")

    print("Вычисление данных...")
    computed_data = data.compute()

    print("Разделение данных по пользователям...")
    train, validation, test = split_user_group(computed_data)

    print("Конвертация обратно в Dask...")
    train_dd = dd.from_pandas(train, npartitions=10)
    validation_dd = dd.from_pandas(validation, npartitions=10)
    test_dd = dd.from_pandas(test, npartitions=10)

    print(f"Разделение завершено: train={len(train)}, val={len(validation)}, test={len(test)}")

    return train_dd, validation_dd, test_dd



def get_user_movie_df(
        data: dd.DataFrame,
) -> dd.DataFrame:
    """Построение user-movie матрицы"""
    data = data.categorize(columns=[
        settings.data.column_names.movieId
    ])
    pivot_data = dd.pivot_table(
        df=data,
        index=settings.data.column_names.userId,
        columns=settings.data.column_names.movieId,
        values=settings.data.column_names.rating,
    ).fillna(0)

    return pivot_data
