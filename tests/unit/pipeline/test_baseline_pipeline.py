import pandas as pd
import pytest

from src.pipelines.baseline_pipeline import BaselinePipeline


@pytest.mark.parametrize(
    "method",
    ["popularity", "mean_rating", "bayesian", "recent", "random"]
)
def test_baseline_pipeline_init_valid_methods(method):
    pipeline = BaselinePipeline(method=method)

    assert pipeline.method == method
    assert pipeline.model_name.startswith("Baseline_")
    assert isinstance(pipeline.model_params, dict)


def test_baseline_pipeline_init_invalid_method():
    with pytest.raises(ValueError):
        BaselinePipeline(method="unknown")


def test_baseline_pipeline_train_sets_dataframes(ratings_ddf):
    pipeline = BaselinePipeline(method="popularity")

    pipeline.train(ratings_ddf)

    assert pipeline.is_trained
    assert pipeline.ratings_df is not None
    assert pipeline.movies_df is not None

    assert isinstance(pipeline.ratings_df, pd.DataFrame)
    assert isinstance(pipeline.movies_df, pd.DataFrame)


def test_baseline_pipeline_uses_external_movies_df(ratings_ddf, movies_df):
    pipeline = BaselinePipeline(
        method="popularity",
        movies_df=movies_df
    )

    pipeline.train(ratings_ddf)

    assert pipeline.movies_df is movies_df


def test_baseline_pipeline_ratings_df_overwritten_on_train(
    ratings_ddf,
    ratings_df
):
    pipeline = BaselinePipeline(
        method="popularity",
        ratings_df=ratings_df
    )

    pipeline.train(ratings_ddf)

    assert len(pipeline.ratings_df) == len(ratings_ddf)
    assert set(pipeline.ratings_df.columns) == set(ratings_df.columns)


def test_baseline_pipeline_recommend_basic(ratings_ddf):
    pipeline = BaselinePipeline(method="popularity")
    pipeline.train(ratings_ddf)

    user_id = 1
    candidates = [1, 1, 1, 1]

    recs = pipeline.recommend(user_id, candidates, k=5)

    assert isinstance(recs, list)
    assert len(recs) <= 5
    assert all(isinstance(i, int) for i in recs)


def test_baseline_pipeline_recommend_filters_items(ratings_ddf):
    pipeline = BaselinePipeline(method="popularity")
    pipeline.train(ratings_ddf)

    candidates = [1, 2]

    recs = pipeline.recommend(user_id=1, items=candidates)

    assert all(i in candidates for i in recs)


def test_baseline_pipeline_recommend_without_train():
    pipeline = BaselinePipeline(method="popularity")

    with pytest.raises(RuntimeError):
        pipeline.recommend(user_id=1, items=[1, 2])

@pytest.mark.parametrize(
    "method",
    ["mean_rating", "bayesian", "recent", "random"]
)
def test_baseline_pipeline_methods_smoke(method, ratings_ddf):
    pipeline = BaselinePipeline(method=method)
    pipeline.train(ratings_ddf)

    recs = pipeline.recommend(user_id=1, items=[])

    assert isinstance(recs, list)



