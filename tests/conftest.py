from __future__ import annotations

import io
from zipfile import ZipFile

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

from src.data_utils.preparing_data import train_validation_test_split_ddf_on_users
from src.ml_models.item_based_cf.model import MLItemBasedCFSimple
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.pipelines.metric_pipeline import MetricPipeline


@pytest.fixture(scope="session")
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

@pytest.fixture(scope="session")
def sample_movies_data(sample_ratings_data):
    movie_ids = sorted(set(sample_ratings_data["movieId"]))

    return {
        "movieId": movie_ids,
        "title": [f"Movie {mid}" for mid in movie_ids],
    }
@pytest.fixture(scope="session")
def movies_df(sample_movies_data):
    return pd.DataFrame(sample_movies_data)

@pytest.fixture(scope="session")
def ratings_ddf(sample_ratings_data):
    df = pd.DataFrame(sample_ratings_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return dd.from_pandas(df)

@pytest.fixture(scope="session")
def ratings_df(sample_ratings_data):
    df = pd.DataFrame(sample_ratings_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@pytest.fixture(scope="session")
def train_validation_test_split_on_users(ratings_ddf):
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
@pytest.fixture
def trained_item_based_model(train_validation_test_split_on_users):
    """ Фикстура с обученной item-based моделью """

    model = MLItemBasedCFSimple()
    model.fit(train_validation_test_split_on_users['train'])

    return {
        'model': model,
        'split_data': train_validation_test_split_on_users,
        'model_type': 'item-based'
    }


@pytest.fixture
def trained_svd_model(train_validation_test_split_on_users):
    """ Фикстура с обученной svd моделью """

    model = MLMatrixFactorizationSVD()
    model.fit(train_validation_test_split_on_users['train'])

    return {
        'model': model,
        'split_data': train_validation_test_split_on_users,
        'model_type': 'svd'
    }




@pytest.fixture
def basic_recommendation_scenario_for_metric_pipeline():
    """Сценарий с обычным примером рекомендаций и релевантных items"""
    return {
        'recommendations': {
            'Model1': [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            'Model2': [[2, 1, 3], [5, 4, 6], [8, 7, 9]]
        },
        'relevant': [[1, 2], [4, 5], [7, 8]]
    }

@pytest.fixture
def perfect_recommendation_scenario_for_metric_pipeline():
    """Сценарий с идеальными рекомендациями."""
    return {
        'recommendations': {
            'PerfectModel': [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        },
        'relevant': [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    }

@pytest.fixture
def empty_recommendation_scenario_for_metric_pipeline():
    """Сценарий с пустыми рекомендациями"""
    return {
        'recommendations': {
            'EmptyModel': [[], [], []]
        },
        'relevant': [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    }

@pytest.fixture
def fake_zip_bytes():
    buf = io.BytesIO()
    with ZipFile(buf, "w") as z:
        for i in range(6):
            z.writestr(f"file_{i}.csv", "a,b,c\n1,2,3\n")
    buf.seek(0)
    return buf.read()


