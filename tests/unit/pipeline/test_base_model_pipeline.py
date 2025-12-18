import dask.dataframe as dd
import pandas as pd
import pytest

from src.pipelines.base_model_pipeline import BaseModelPipeline


class DummyPipeline(BaseModelPipeline):
    def _create_model(self):
        return {"dummy": True}

    def _fit_model(self, train_data: dd.DataFrame):
        pass

    def _recommend_impl(self, user_id: int, items):
        return list(items)

@pytest.fixture
def dummy_pipeline(tmp_path):
    return DummyPipeline(
        model_name="dummy_model",
        models_dir=tmp_path,
        overwrite_model=True
    )


def test_train_creates_files(dummy_pipeline, ratings_ddf):
    dummy_pipeline.train(ratings_ddf)

    assert dummy_pipeline.is_trained is True
    assert dummy_pipeline.model is not None
    assert dummy_pipeline.model_path.exists()
    assert dummy_pipeline.info_path.exists()

def test_train_loads_existing_model(dummy_pipeline, ratings_ddf):
    dummy_pipeline.train(ratings_ddf)

    model_first = dummy_pipeline.model

    dummy_pipeline.model = None
    dummy_pipeline.is_trained = False

    dummy_pipeline.train(ratings_ddf)

    assert dummy_pipeline.is_trained is True
    assert dummy_pipeline.model == model_first


def test_recommend_returns_items(dummy_pipeline, ratings_ddf):
    dummy_pipeline.train(ratings_ddf)

    items = [1, 2, 3]
    recs = dummy_pipeline.recommend(user_id=1, items=items)

    assert recs == items

def test_recommend_raises_if_not_trained(dummy_pipeline):
    with pytest.raises(RuntimeError):
        dummy_pipeline.recommend(1, [1, 2])

def test_collect_recommendations(dummy_pipeline, ratings_ddf):
    dummy_pipeline.train(ratings_ddf)

    test_cases = [
        (1, [1, 2, 3], [2]),
        (2, [4, 5], [5]),
    ]
    all_recs, all_rel = dummy_pipeline.collect_recommendations(test_cases)

    assert len(all_recs) == 2
    assert len(all_rel) == 2

    assert all_recs[0] == [1, 2, 3]
    assert all_rel[0] == [2]

def test_cleanup(dummy_pipeline, ratings_ddf):
    dummy_pipeline.train(ratings_ddf)

    assert dummy_pipeline.model_dir.exists()

    dummy_pipeline.cleanup()

    assert not dummy_pipeline.model_dir.exists()
    assert dummy_pipeline.model is None
    assert dummy_pipeline.is_trained is False
