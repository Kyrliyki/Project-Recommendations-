import pytest
from pathlib import Path
import dask.dataframe as dd
import pandas as pd

from src.data_utils.split_strategies import SplitStrategy
from src.utils.policy import SplitPolicy, DownloadPolicy
from src.data_utils.data_loader import DataLoader



class DummySplitStrategy:
    def get_name(self):
        return "dummy"

    def split(self, df):
        return df, df, df


@pytest.fixture
def data_loader(tmp_path):
    raw_dir = tmp_path / "raw"
    splits_dir = tmp_path / "splits"

    return DataLoader(
        raw_csv_path=raw_dir,
        splits_dir=splits_dir,
        download_policy=DownloadPolicy.IF_MISSING,
        split_policy=SplitPolicy.REUSE,
        split_strategy=DummySplitStrategy(),
        download_url="http://example.com/data.csv",
    )


def test_ensure_raw_data_csv_exists(monkeypatch, data_loader):
    """Проверка на скачивание файла в случае если он есть"""
    data_loader.raw_csv_path.parent.mkdir(parents=True, exist_ok=True)
    data_loader.raw_csv_path.write_text("dummy")
    called = False

    def fake_download():
        nonlocal called
        called = True

    monkeypatch.setattr(data_loader, "_download", fake_download)

    data_loader._ensure_raw_data()

    assert called is False


def test_ensure_raw_data_never_policy_raises(data_loader):
    data_loader.download_policy = DownloadPolicy.NEVER

    with pytest.raises(FileNotFoundError):
        data_loader._ensure_raw_data()



def test_ensure_raw_data_triggers_download(monkeypatch, data_loader):
    called = False

    def fake_download():
        nonlocal called
        called = True

    monkeypatch.setattr(data_loader, "_download", fake_download)

    data_loader._ensure_raw_data()

    assert called is True


def test_download_without_url_raises(tmp_path):
    loader = DataLoader(
        raw_csv_path=tmp_path / "raw",
        splits_dir=tmp_path / "splits",
        download_policy=DownloadPolicy.ALWAYS,
        split_strategy=DummySplitStrategy(),
        download_url=None,
    )

    with pytest.raises(ValueError):
        loader._download()


def test_ensure_splits_reuse(monkeypatch, data_loader):
    data_loader.train_path.touch()
    data_loader.val_path.touch()
    data_loader.test_path.touch()

    called = False

    def fake_create():
        nonlocal called
        called = True

    monkeypatch.setattr(data_loader, "_create_and_save_splits", fake_create)

    data_loader._ensure_splits()

    assert called is False


def test_ensure_splits_creates(monkeypatch, data_loader):
    called = False

    def fake_create():
        nonlocal called
        called = True

    monkeypatch.setattr(data_loader, "_create_and_save_splits", fake_create)

    data_loader._ensure_splits()

    assert called is True


def test_create_and_save_splits(monkeypatch, data_loader):
    # фиктивный dask dataframe
    df = dd.from_pandas(
        __import__("pandas").DataFrame({"timestamp": [1, 2, 3]}),
        npartitions=1,
    )

    monkeypatch.setattr(dd, "read_csv", lambda *a, **k: df)

    saved = []

    def fake_to_parquet(self, path, overwrite=False):
        saved.append(path)

    monkeypatch.setattr(dd.DataFrame, "to_parquet", fake_to_parquet)

    data_loader._create_and_save_splits()

    assert data_loader.train_path in saved
    assert data_loader.val_path in saved
    assert data_loader.test_path in saved


def test_load_splits(monkeypatch, data_loader):
    df = dd.from_pandas(
        __import__("pandas").DataFrame({"a": [1, 2]}),
        npartitions=1,
    )

    monkeypatch.setattr(dd, "read_parquet", lambda *a, **k: df)

    train, val, test = data_loader._load_splits()

    assert train is df
    assert val is df
    assert test is df


def test_load_pipeline(monkeypatch, data_loader):
    monkeypatch.setattr(data_loader, "_ensure_raw_data", lambda: None)
    monkeypatch.setattr(data_loader, "_ensure_splits", lambda: None)

    df = dd.from_pandas(
        __import__("pandas").DataFrame({"a": [1]}),
        npartitions=1,
    )

    monkeypatch.setattr(dd, "read_parquet", lambda *a, **k: df)

    train, val, test = data_loader.load()

    assert train is df
    assert val is df
    assert test is df
