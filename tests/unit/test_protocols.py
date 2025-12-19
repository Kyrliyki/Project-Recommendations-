import numbers

import dask
import pytest

from src.evaluation.protocols.all_to_many import (
    AllPositivesToManyNegativesProtocol
)
from src.evaluation.protocols.one_to_many import OnePositiveToManyNegativesProtocol
from src.evaluation.protocols.only_all_relevant import OnlyAllPositives


def test_prepare_test_cases(train_validation_test_split_on_users):

    train = train_validation_test_split_on_users["train"]
    validation = train_validation_test_split_on_users["validation"]

    train_pd, validation_pd = dask.compute(
        train_validation_test_split_on_users["train"],
        train_validation_test_split_on_users["validation"]
    )

    protocol = AllPositivesToManyNegativesProtocol(
        num_negatives_per_positive=2,
        threshold=3
    )

    test_cases = protocol.prepare_test_cases(train, validation)

    assert isinstance(test_cases, list)
    assert len(test_cases) > 0

    for user, candidates, relevant_items in test_cases:
        assert isinstance(user, int)
        assert isinstance(candidates, list)
        assert isinstance(relevant_items, list)
        assert len(relevant_items) > 0
        assert set(relevant_items).issubset(set(candidates))
        assert len(candidates) == len(relevant_items) * (
                protocol.num_negatives_per_positive + 1
        )

        seen_items = set(
            train_pd[train_pd["userId"] == user]["movieId"]
        ) | set(
            validation_pd[validation_pd["userId"] == user]["movieId"]
        )

        negatives = set(candidates) - set(relevant_items)

        assert negatives.isdisjoint(seen_items)

    protocol = OnlyAllPositives(
        threshold=3,
        min_relevant_items=2
    )

    test_cases = protocol.prepare_test_cases(train, validation)
    assert len(test_cases) > 0

    for user, all_items, relevant_items in test_cases:
        assert isinstance(user, int)
        assert isinstance(all_items, list)
        assert isinstance(relevant_items, list)

def test_n_users_limit(train_validation_test_split_on_users):
    train = train_validation_test_split_on_users["train"]
    validation = train_validation_test_split_on_users["validation"]

    protocol = AllPositivesToManyNegativesProtocol(
        num_negatives_per_positive=2,
        threshold=3,
        n_users=2
    )
    test_cases = protocol.prepare_test_cases(train, validation)
    assert len(test_cases) <= 2

    protocol = OnePositiveToManyNegativesProtocol(
        num_negatives=2,
        threshold=3,
        n_users=2
    )

    test_cases = protocol.prepare_test_cases(train, validation)
    users_in_test_cases = {user for user, _, _ in test_cases}
    assert len(users_in_test_cases) <= 2

    protocol = OnlyAllPositives(
        threshold=3,
        min_relevant_items=2,
        n_users=5
    )

    test_cases = protocol.prepare_test_cases(train, validation)
    assert len(test_cases) <= 5

def test_raises_error_when_no_positive_ratings(train_validation_test_split_on_users):
    train = train_validation_test_split_on_users["train"]
    validation = train_validation_test_split_on_users["validation"]

    protocol = AllPositivesToManyNegativesProtocol(
        threshold=10
    )

    with pytest.raises(ValueError, match="нет релевантных"):
        protocol.prepare_test_cases(train, validation)

    protocol = OnePositiveToManyNegativesProtocol(
        threshold=10
    )

    with pytest.raises(ValueError, match="нет релевантных"):
        protocol.prepare_test_cases(train, validation)

    protocol = OnlyAllPositives(
        threshold=10
    )

    with pytest.raises(ValueError, match="нет релевантных"):
        protocol.prepare_test_cases(train, validation)

def test_raises_error_when_no_users_meet_min_relevant_movie(train_validation_test_split_on_users):
    train = train_validation_test_split_on_users["train"]
    validation = train_validation_test_split_on_users["validation"]

    protocol = OnlyAllPositives(
        threshold=3,
        min_relevant_items=99999
    )

    with pytest.raises(ValueError, match="Нет пользователей"):
        protocol.prepare_test_cases(train, validation)

def test_min_relevant_items_constraint(train_validation_test_split_on_users):
    train = train_validation_test_split_on_users["train"]
    validation = train_validation_test_split_on_users["validation"]

    protocol = OnlyAllPositives(
        threshold=3,
        min_relevant_items=3
    )

    test_cases = protocol.prepare_test_cases(train, validation)

    validation_pd = validation.compute()

    for user, _, relevant_items in test_cases:
        real_count = len(
            validation_pd[
                (validation_pd["userId"] == user) &
                (validation_pd["rating"] >= 3)
            ]
        )
        assert real_count >= 3



class DummyModel:
    def predict_rating(self, user_id, item_id):
        return 3

def test_collect_rating_predictions(train_validation_test_split_on_users):
    validation = train_validation_test_split_on_users["validation"]

    protocol = OnlyAllPositives(
        threshold=3,
        min_relevant_items=1,
        n_users=3
    )

    test_cases = protocol.prepare_test_cases(
        train_validation_test_split_on_users["train"],
        validation
    )

    model = DummyModel()

    y_true, y_pred = protocol.collect_rating_predictions(
        model, test_cases, validation
    )

    assert len(y_true) == len(y_pred)
    assert len(y_true) > 0

    for yt, yp in zip(y_true, y_pred):
        assert len(yt) == len(yp)
        assert all(isinstance(v, numbers.Real) for v in yp)
