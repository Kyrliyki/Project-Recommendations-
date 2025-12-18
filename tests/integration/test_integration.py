import time

import pandas as pd
import pytest

from src.data_utils.data_loader import DataLoader
from src.evaluation.protocols.only_all_relevant import OnlyAllPositives
from src.pipelines.metric_pipeline import MetricPipeline
from src.pipelines.svd_pipeline import SVDPipeline
from src.utils.policy import DownloadPolicy, SplitPolicy


def test_dataloader_time_split_integration(tmp_path, ratings_ddf):
    raw_dir = tmp_path / "raw"
    splits_dir = tmp_path / "splits"
    raw_dir.mkdir()


    csv_path = raw_dir / "rating.csv"
    ratings_ddf.to_csv(csv_path, index=False, single_file=True)

    loader = DataLoader(
        raw_csv_path=raw_dir,
        splits_dir=splits_dir,
        split_strategy="user",
        split_strategy_kwargs={
            "test_ratio": 0.1,
            "validation_ratio": 0.2,
        },
        download_policy=DownloadPolicy.NEVER,
    )

    train, val, test = loader.load()

    train_df = train.compute()
    val_df = val.compute()
    test_df = test.compute()


    assert loader.train_path.exists()
    assert loader.val_path.exists()
    assert loader.test_path.exists()


    assert len(train_df) + len(val_df) + len(test_df) == len(ratings_ddf)


    for uid, group in pd.concat([
        train_df.assign(split="train"),
        val_df.assign(split="val"),
        test_df.assign(split="test"),
    ]).groupby("userId"):

        train_ts = group[group.split == "train"]["timestamp"]
        val_ts = group[group.split == "val"]["timestamp"]
        test_ts = group[group.split == "test"]["timestamp"]

        if not train_ts.empty and not val_ts.empty:
            assert train_ts.max() <= val_ts.min()

        if not val_ts.empty and not test_ts.empty:
            assert val_ts.max() <= test_ts.min()


def test_dataloader_reuse_policy(tmp_path, ratings_ddf):
    raw_dir = tmp_path / "raw"
    splits_dir = tmp_path / "splits"
    raw_dir.mkdir()



    ratings_ddf.to_csv(raw_dir / "rating.csv", index=False, single_file=True)

    loader = DataLoader(
        raw_csv_path=raw_dir,
        splits_dir=splits_dir,
        split_strategy="user",
        split_strategy_kwargs={"test_ratio": 0.1, "validation_ratio": 0.2},
        split_policy=SplitPolicy.REUSE,
        download_policy=DownloadPolicy.NEVER,
    )

    loader.load()

    before = loader.train_path.stat().st_mtime
    loader.load()
    after = loader.train_path.stat().st_mtime

    assert before == after


def test_dataloader_never_policy_raises(tmp_path):
    loader = DataLoader(
        raw_csv_path=tmp_path / "raw",
        splits_dir=tmp_path / "splits",
        download_policy=DownloadPolicy.NEVER,
        split_strategy_kwargs = {"test_ratio": 0.1, "validation_ratio": 0.2},
    )

    with pytest.raises(FileNotFoundError):
        loader.load()




def test_svd_pipeline_overwrite_model_rewrites_file(ratings_ddf, tmp_path):
    pipeline = SVDPipeline(models_dir=tmp_path)
    pipeline.train(ratings_ddf)

    model_path = tmp_path / "svd" / "svd.pkl"
    assert model_path.exists()

    first_mtime = model_path.stat().st_mtime
    time.sleep(1)

    pipeline_overwrite = SVDPipeline(
        models_dir=tmp_path,
        overwrite_model=True
    )
    pipeline_overwrite.train(ratings_ddf)

    second_mtime = model_path.stat().st_mtime

    assert second_mtime > first_mtime, "Модель не была перезаписана"

def test_full_model_pipeline(tmp_path, ratings_ddf):
    raw_dir = tmp_path / "raw"
    splits_dir = tmp_path / "splits"
    raw_dir.mkdir()

    ratings_ddf.to_csv(raw_dir / "rating.csv", index=False, single_file=True)

    loader = DataLoader(
        raw_csv_path=raw_dir,
        splits_dir=splits_dir,
        split_strategy="user",
        split_strategy_kwargs={
            "test_ratio": 0.1,
            "validation_ratio": 0.2,
        },
        download_policy=DownloadPolicy.NEVER,
    )

    train, _, _ = loader.load()

    user_id = train["userId"].compute().iloc[0]
    model = SVDPipeline(models_dir=tmp_path / "svd")
    model.train(train)

    assert model.is_trained



    recs = model.recommend(user_id=user_id)
    assert isinstance(recs, list)

    user_seen_items = set(
        train[train["userId"] == user_id]["movieId"]
        .compute()
        .unique()
    )

    assert not any(item in user_seen_items for item in recs)

def test_model_and_protocol_integration(train_validation_test_split_on_users, tmp_path):
    train_df = train_validation_test_split_on_users['train']
    test_df = train_validation_test_split_on_users['test']
    model = SVDPipeline(models_dir=tmp_path / "svd")
    model.train(train_df)

    protocol = OnlyAllPositives(
        n_users=10,
        threshold=2.0,
        min_relevant_items=1
    )

    test_cases = protocol.prepare_test_cases(train_df, test_df)

    assert len(test_cases) > 0

    for case in test_cases:
        assert isinstance(case, tuple)
        assert len(case) == 3

        user_id, candidates, relevant = case

        assert isinstance(user_id, int)
        assert isinstance(candidates, list)
        assert isinstance(relevant, list)


def test_recommendation_collection_integration(train_validation_test_split_on_users, tmp_path):
    train_df = train_validation_test_split_on_users['train']
    test_df = train_validation_test_split_on_users['test']
    model = SVDPipeline(models_dir=tmp_path / "svd")
    model.train(train_df)

    protocol = OnlyAllPositives(
        n_users=10,
        threshold=2.0,
        min_relevant_items=1
    )

    test_cases = protocol.prepare_test_cases(train_df, test_df)
    recommendations, relevant_items = model.collect_recommendations(test_cases)

    assert len(recommendations) == len(relevant_items)

    for recs, relevant in zip(recommendations, relevant_items):
        assert isinstance(recs, list)
        assert isinstance(relevant, list)

def test_metric_pipeline_integration(train_validation_test_split_on_users, tmp_path):
    train_df = train_validation_test_split_on_users['train']
    test_df = train_validation_test_split_on_users['test']
    model = SVDPipeline(models_dir=tmp_path / "svd")
    model.train(train_df)

    protocol = OnlyAllPositives(
        n_users=10,
        threshold=2.0,
        min_relevant_items=1
    )

    test_cases = protocol.prepare_test_cases(train_df, test_df)
    recommendations, relevant_items = model.collect_recommendations(test_cases)

    metric_pipeline = MetricPipeline(
        k_list=[5, 10],
        metrics=["Precision", "Recall", "MAP", "NDCG"]
    )

    df = metric_pipeline.run(
        model_recommendations={"SVD": recommendations},
        relevant_items=relevant_items
    )

    assert not df.empty