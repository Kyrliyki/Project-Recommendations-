import pytest

from src.ml_models.item_based_cf.model import MLItemBasedCFSimple
from src.pipelines.item_based_pipeline import ItemBasedPipeline


def test_item_based_pipeline_init_default(tmp_path):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)

    assert pipeline.model_name == "ItemBased"
    assert pipeline.model is None
    assert not pipeline.is_trained


def test_item_based_pipeline_train(ratings_ddf, tmp_path, monkeypatch):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)

    fit_called = False

    def fake_fit(self, data):
        nonlocal fit_called
        fit_called = True

    monkeypatch.setattr(
        MLItemBasedCFSimple,
        "fit",
        fake_fit
    )

    pipeline.train(ratings_ddf)

    assert pipeline.is_trained
    assert pipeline.model is not None
    assert fit_called is True


def test_item_based_pipeline_train_load_existing_model(
    ratings_ddf, tmp_path, monkeypatch
):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)
    pipeline.train(ratings_ddf)

    fit_called = False

    def fake_fit(self, data):
        nonlocal fit_called
        fit_called = True

    monkeypatch.setattr(
        ItemBasedPipeline,
        "_fit_model",
        fake_fit
    )

    pipeline2 = ItemBasedPipeline(models_dir=tmp_path)
    pipeline2.train(ratings_ddf)

    assert pipeline2.is_trained
    assert fit_called is False, "Модель была переобучена вместо загрузки"


def test_item_based_pipeline_recommend(monkeypatch, tmp_path):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)
    pipeline.is_trained = True

    class FakeModel:
        def getting_recommended_movies(self, user_id, movies_list):
            return [1, 2, 3, 4]

    pipeline.model = FakeModel()

    recs = pipeline.recommend(user_id=1, items=[1, 2, 3], k=2)

    assert recs == [1, 2]

def test_item_based_pipeline_recommend_without_k(monkeypatch, tmp_path):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)
    pipeline.is_trained = True

    class FakeModel:
        def getting_recommended_movies(self, user_id, movies_list):
            return [1, 2, 3]

    pipeline.model = FakeModel()

    recs = pipeline.recommend(user_id=1, items=[1, 2, 3])

    assert recs == [1, 2, 3]


def test_item_based_pipeline_predict_rating(monkeypatch, tmp_path):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)
    pipeline.is_trained = True

    class FakePrediction:
        est = 4

    class FakeModel:
        def predict(self, user_id, item_id):
            return FakePrediction()

    pipeline.model = FakeModel()

    rating = pipeline.predict_rating(user_id=1, item_id=2)

    assert rating == 4


def test_item_based_pipeline_without_train(tmp_path):
    pipeline = ItemBasedPipeline(models_dir=tmp_path)

    with pytest.raises(RuntimeError):
        pipeline.recommend(user_id=1, items=[1, 2])

    with pytest.raises(ValueError):
        pipeline.predict_rating(user_id=1, item_id=1)


