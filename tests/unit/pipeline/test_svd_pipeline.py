import time

import pytest

from src.pipelines.svd_pipeline import SVDPipeline


def test_svd_pipeline_init_default(tmp_path):
    pipeline = SVDPipeline(models_dir=tmp_path)

    assert pipeline.model_name == "SVD"
    assert isinstance(pipeline.model_params, dict)
    assert pipeline.model is None
    assert not pipeline.is_trained

def test_svd_pipeline_init_with_params(tmp_path):
    params = {
        "n_factors": 10,
        "n_epochs": 5,
    }

    pipeline = SVDPipeline(model_params=params, models_dir=tmp_path)

    assert pipeline.model_params["n_factors"] == 10
    assert pipeline.model_params["n_epochs"] == 5

def test_svd_pipeline_train(ratings_ddf, tmp_path,  monkeypatch):
    pipeline = SVDPipeline(models_dir=tmp_path)

    pipeline.train(ratings_ddf)

    assert pipeline.is_trained
    assert pipeline.model is not None
    assert pipeline.model.model is not None

    fit_called = False

    def fake_fit(self, train_data):
        nonlocal fit_called
        fit_called = True


    monkeypatch.setattr(
        SVDPipeline,
        "_fit_model",
        fake_fit
    )

    pipeline2 = SVDPipeline(models_dir=tmp_path)
    pipeline2.train(ratings_ddf)

    assert pipeline2.is_trained
    assert pipeline2.model is not None
    assert fit_called is False, "Модель была переобучена вместо загрузки"

def test_svd_pipeline_recommend(ratings_ddf, tmp_path):
    pipeline = SVDPipeline(models_dir=tmp_path)
    pipeline.train(ratings_ddf)

    user_id = 1
    candidates = [1, 2, 3, 4]

    recs = pipeline.recommend(user_id=user_id, items=candidates, k=3)

    assert isinstance(recs, list)
    assert len(recs) <= 3
    assert all(isinstance(i, int) for i in recs)

def test_svd_pipeline_recommend_without_k(monkeypatch, tmp_path):
    pipeline = SVDPipeline(models_dir=tmp_path)
    pipeline.is_trained = True

    expected_recs = [1, 2, 3]

    def fake_recommend_impl(self, user_id, items):
        return expected_recs.copy()

    monkeypatch.setattr(
        SVDPipeline,
        "_recommend_impl",
        fake_recommend_impl
    )

    recs = pipeline.recommend(user_id=1, items=[1, 2, 3])

    assert recs == expected_recs


def test_svd_pipeline_without_train(tmp_path):
    pipeline = SVDPipeline(models_dir=tmp_path)

    with pytest.raises(RuntimeError):
        pipeline.recommend(user_id=1, items=[1, 2])

    with pytest.raises(ValueError):
        pipeline.predict_rating(user_id=1, item_id=1)


