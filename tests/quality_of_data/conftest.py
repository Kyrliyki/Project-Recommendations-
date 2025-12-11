from pathlib import Path

import pytest
import dask.dataframe as dd

from src.data_utils.preparing_data import train_validation_test_split_ddf_on_users, train_validation_test_split_ddf
from src.utils.config import settings


@pytest.fixture(scope="session")
def real_ratings_ddf():
    """Реальные данные о рейтингах для Quality of Data tests"""
    try:

        if not Path(settings.data.path_to_rating_csv).exists():
            pytest.skip(f"Файл не найден: {settings.data.path_to_rating_csv}")

        ratings = dd.read_csv(settings.data.path_to_rating_csv)
        ratings['timestamp'] = dd.to_datetime(ratings['timestamp'])

        ratings = ratings.astype({
            'userId': 'int64',
            'movieId': 'int64',
            'rating': 'float64',
            'timestamp': 'datetime64[ns]'
        })

        return ratings


    except Exception as e:
        pytest.fail(f"Не удалось загрузить  данные: {e}")


@pytest.fixture(scope="session")
def real_movies_ddf():
    """Реальные данные о фильмах для Quality of Data tests"""
    try:
        movies = dd.read_csv(settings.data.path_to_movie_csv)
        return movies
    except Exception as e:
        pytest.fail(f"Не удалось загрузить данные о фильмах: {e}")




@pytest.fixture(scope="session")
def train_validation_test_split_on_real_data(ratings_ddf):
    test_ratio = 0.1
    validation_ratio = 0.1

    train, validation, test = train_validation_test_split_ddf(
        ratings_ddf,
        test_ratio=test_ratio,
        validation_ratio=validation_ratio
    )

    return {
        'train': train,
        'validation': validation,
        'test': test,
        'params': {'test_ratio': test_ratio, 'validation_ratio': validation_ratio}
    }
@pytest.fixture(scope="session")
def train_validation_test_split_on_users_and_real_data(ratings_ddf):
    test_ratio = 0.1
    validation_ratio = 0.1

    train, validation, test = train_validation_test_split_ddf_on_users(
        ratings_ddf,
        test_ratio=test_ratio,
        validation_ratio=validation_ratio
    )

    return {
        'train': train,
        'validation': validation,
        'test': test,
        'params': {'test_ratio': test_ratio, 'validation_ratio': validation_ratio}
    }


