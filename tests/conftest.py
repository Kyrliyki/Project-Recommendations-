from __future__ import annotations
import pytest
import pandas as pd
import numpy as np
import dask.dataframe as dd
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Iterator, TypedDict, Protocol
from collections.abc import Callable
from dask.distributed import Client, LocalCluster
import sys

@pytest.fixture
def sample_ratings_data():
    np.random.seed(17)
    n_records = 1000

    user_ids = np.random.randint(1, 21, n_records).tolist()
    movie_ids = np.random.randint(101, 151, n_records).tolist()
    ratings = np.random.choice(np.arange(0.5, 5.1, 0.5), n_records).tolist()

    base_date = datetime(2004, 1, 1)
    timestamps = [
        (base_date + pd.Timedelta(days=np.random.randint(0, 365),
                                  hours=np.random.randint(0, 24))
         ).strftime('%Y-%m-%d %H:%M:%S')
        for _ in range(n_records)
    ]
    return {
        'userId': user_ids,
        'movieId': movie_ids,
        'rating': ratings,
        'timestamp': timestamps
    }

@pytest.fixture
def ratings_ddf(sample_ratings_data):
    df = pd.DataFrame(sample_ratings_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return dd.from_pandas(df)


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def ratings_csv_file(sample_ratings_data, temp_dir):
    csv_path = temp_dir / 'ratings.csv'
    pd.DataFrame(sample_ratings_data).to_csv(csv_path, index=False)
    return csv_path








# @pytest.fixture(scope="session")
# def dask_client() -> Iterator[Client]:
#     """Modern async context manager for Dask client"""
#     cluster = LocalCluster(
#         n_workers=2,
#         threads_per_worker=1,
#         processes=True,
#         memory_limit='1GB',
#         silence_logs=50
#     )
#
#     async with Client(cluster, asynchronous=True) as client:
#         yield client


# @pytest.fixture
# def sample_ratings_data(test_config: TestConfig) -> pd.DataFrame:
#     """Modern data generation using vectorized operations"""
#     np.random.seed(test_config.random_seed)
#
#
#     user_ids = np.repeat(
#         np.arange(1, test_config.n_users + 1),
#         np.random.randint(5, 50, test_config.n_users)
#     )
#
#     movie_ids = np.random.randint(
#         1, test_config.n_movies + 1,
#         size=len(user_ids)
#     )
#
#     ratings = np.random.choice(
#         [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
#         size=len(user_ids)
#     )
#
#     base_date = datetime(2023, 1, 1)
#     timestamps = base_date + pd.to_timedelta(
#         np.random.randint(0, 365 * 24 * 3600, len(user_ids)),
#         unit='s'
#     )
#
#     return pd.DataFrame({
#         'userId': user_ids,
#         'movieId': movie_ids,
#         'rating': ratings,
#         'timestamp': timestamps
#     }).drop_duplicates(['userId', 'movieId'])


