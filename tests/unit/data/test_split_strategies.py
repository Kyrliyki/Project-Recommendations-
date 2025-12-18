import pytest
import dask.dataframe as dd
import pandas as pd

from src.data_utils import split_strategies
from src.data_utils.preparing_data import train_validation_test_split_ddf, train_validation_test_split_ddf_on_users
from src.data_utils.split_strategies import TimeSplitStrategy, UserSplitStrategy, get_split_strategy


def test_time_split_strategy_calls_function(monkeypatch, ratings_ddf):
    called = {}

    def fake_split(data, validation_ratio, test_ratio):
        called["data"] = data
        called["validation_ratio"] = validation_ratio
        called["test_ratio"] = test_ratio
        return "train", "validation", "test"

    monkeypatch.setattr(
        split_strategies,
        "train_validation_test_split_ddf",
        fake_split
    )

    strategy = TimeSplitStrategy(validation_ratio=0.1, test_ratio=0.2)
    result = strategy.split(ratings_ddf)

    assert result == ("train", "validation", "test")
    assert called["data"] is ratings_ddf
    assert called["validation_ratio"] == 0.1
    assert called["test_ratio"] == 0.2


def test_user_split_strategy_calls_function(monkeypatch, ratings_ddf):
    called = {}

    def fake_split(data, validation_ratio, test_ratio):
        called["data"] = data
        called["validation_ratio"] = validation_ratio
        called["test_ratio"] = test_ratio
        return "train", "validation", "test"

    monkeypatch.setattr(
        split_strategies,
        "train_validation_test_split_ddf_on_users",
        fake_split
    )

    strategy = UserSplitStrategy(validation_ratio=0.1, test_ratio=0.2)
    result = strategy.split(ratings_ddf)

    assert result == ("train", "validation", "test")
    assert called["data"] is ratings_ddf
    assert called["validation_ratio"] == 0.1
    assert called["test_ratio"] == 0.2

def test_strategy_names():
    assert TimeSplitStrategy(0.1, 0.1).get_name() == "time_based"
    assert UserSplitStrategy(0.1, 0.1).get_name() == "user_based"

def test_strategy_params():
    time = TimeSplitStrategy(0.2, 0.1)
    user = UserSplitStrategy(0.3, 0.2)

    assert time.get_params() == {
        "test_ratio": 0.2,
        "validation_ratio": 0.1,
        "type": "time_based"
    }

    assert user.get_params() == {
        "test_ratio": 0.3,
        "validation_ratio": 0.2,
        "type": "user_based"
    }

def test_get_split_strategy_valid():
    strat_time = get_split_strategy(
        "time",
        test_ratio=0.1,
        validation_ratio=0.1
    )

    strat_user = get_split_strategy(
        "user",
        test_ratio=0.1,
        validation_ratio=0.1
    )

    assert isinstance(strat_time, TimeSplitStrategy)
    assert strat_time.test_ratio == 0.1
    assert strat_time.validation_ratio == 0.1

    assert isinstance(strat_user, UserSplitStrategy)
    assert strat_user.test_ratio == 0.1
    assert strat_user.validation_ratio == 0.1


def test_get_split_strategy_unknown():
    with pytest.raises(ValueError) as exc:
        get_split_strategy("unknown", test_ratio=0.1, validation_ratio=0.1)

    assert "Unknown split strategy" in str(exc.value)
