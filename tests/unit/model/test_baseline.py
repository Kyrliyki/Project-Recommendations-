import pytest
import pandas as pd
from src.baseline.baseline import Baseline


def test_get_seen_movie_ids(ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    seen = Baseline._get_seen_movie_ids(ratings_df, user_id)

    expected = set(
        ratings_df.loc[ratings_df["userId"] == user_id, "movieId"]
    )

    assert seen == expected


def test_bayesian_mean_baseline_basic(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs = Baseline.bayesian_mean_baseline(
        movies_df,
        ratings_df,
        user_id=user_id,
        n_recommendations=10,
        m=10,
    )

    assert isinstance(recs, list)
    assert len(recs) <= 10

def test_bayesian_mean_baseline_excludes_seen(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    seen = set(
        ratings_df.loc[ratings_df["userId"] == user_id, "movieId"]
    )

    recs = Baseline.bayesian_mean_baseline(
        movies_df, ratings_df, user_id
    )

    assert not seen.intersection(recs)

def test_recent_popularity_baseline_basic(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs = Baseline.recent_popularity_baseline(
        movies_df,
        ratings_df,
        user_id=user_id,
        window_days=180,
    )

    assert isinstance(recs, list)


def test_recent_popularity_baseline_excludes_seen(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    seen = set(
        ratings_df.loc[ratings_df["userId"] == user_id, "movieId"]
    )

    recs = Baseline.recent_popularity_baseline(
        movies_df, ratings_df, user_id
    )

    assert not seen.intersection(recs)


def test_recent_popularity_baseline_no_timestamp(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]
    rating_no_ts = ratings_df.drop(columns=["timestamp"])

    with pytest.raises(ValueError):
        Baseline.recent_popularity_baseline(
            movies_df, rating_no_ts, user_id
        )


def test_random_baseline_deterministic(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs1 = Baseline.random_baseline(
        movies_df,
        ratings_df,
        user_id,
        n_recommendations=5,
        random_state=42,
    )

    recs2 = Baseline.random_baseline(
        movies_df,
        ratings_df,
        user_id,
        n_recommendations=5,
        random_state=42,
    )

    assert recs1 == recs2


def test_random_baseline_excludes_seen(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    seen = set(
        ratings_df.loc[ratings_df["userId"] == user_id, "movieId"]
    )

    recs = Baseline.random_baseline(
        movies_df, ratings_df, user_id
    )

    assert not seen.intersection(recs)



def test_popularity_baseline_basic(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs = Baseline.popularity_baseline(
        movies_df, ratings_df, user_id
    )

    assert isinstance(recs, list)


def test_popularity_baseline_sorted(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs = Baseline.popularity_baseline(
        movies_df, ratings_df, user_id, n_recommendations=5
    )

    # частота рейтингов убывает
    counts = (
        ratings_df.groupby("movieId")["userId"]
        .count()
        .to_dict()
    )

    freqs = [counts[mid] for mid in recs]
    assert freqs == sorted(freqs, reverse=True)


def test_mean_rating_baseline_basic(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs = Baseline.mean_rating_baseline(
        movies_df,
        ratings_df,
        user_id,
        min_n_ratings=5,
    )

    assert isinstance(recs, list)


def test_mean_rating_baseline_respects_min_n_ratings(movies_df, ratings_df):
    user_id = ratings_df["userId"].iloc[0]

    recs = Baseline.mean_rating_baseline(
        movies_df,
        ratings_df,
        user_id,
        min_n_ratings=10,
    )

    counts = ratings_df.groupby("movieId")["rating"].count()

    for mid in recs:
        assert counts[mid] >= 10
