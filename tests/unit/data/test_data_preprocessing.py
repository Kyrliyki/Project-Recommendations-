import pandas as pd
import pytest
from src.data_utils.preparing_data import (
    train_validation_test_split_ddf, train_validation_test_split_ddf_on_users, \
    split_user_group, download_csv, requests
)



@pytest.mark.user_group
def test_split_user_group_no_data_loss(ratings_df):
    train, val, test = split_user_group(
        ratings_df,
        test_ratio=0.3,
        validation_ratio=0.2
    )

    assert len(train) + len(val) + len(test) == len(ratings_df)


@pytest.mark.train_test_split_ddf_test()
@pytest.mark.parametrize("test_ratio,validation_ratio", [
    (0.1, 0.1),
    (0.15, 0.15),
    (0.2, 0.1),
    (0.05, 0.15),
])
def test_time_split_function(ratings_ddf, test_ratio, validation_ratio):
    """Тест  функции разделения данных по времени с разными размерами"""

    train_ratio = 1 - test_ratio - validation_ratio

    train, validation, test = train_validation_test_split_ddf(
        ratings_ddf,
        test_ratio=test_ratio,
        validation_ratio=validation_ratio,
    )

    tolerance = 0.02



    total_count = ratings_ddf.shape[0].compute()
    train_count = train.shape[0].compute()
    val_count = validation.shape[0].compute()
    test_count = test.shape[0].compute()

    actual_train_ratio = train_count / total_count
    actual_val_ratio = val_count / total_count
    actual_test_ratio = test_count / total_count

    assert abs(actual_train_ratio - train_ratio) < tolerance, f"Train ratio {actual_train_ratio} != {train_ratio}"
    assert abs(actual_val_ratio - validation_ratio) < tolerance, f"Validation ratio {actual_val_ratio} != {validation_ratio}"
    assert abs(actual_test_ratio - test_ratio) < tolerance, f"Test ratio {actual_test_ratio} != {test_ratio}"







@pytest.mark.train_test_split_ddf_per_user_test()
@pytest.mark.parametrize("test_ratio,validation_ratio", [
    (0.1, 0.1),
    (0.15, 0.15),
    (0.2, 0.1),
    (0.05, 0.15),
    (0.0, 0.2),
    (0.2, 0.0),
])
def test_split_proportions_per_user(ratings_ddf, test_ratio, validation_ratio):
    """ Проверка пропорций разделения для каждого пользователя """

    train, validation, test = train_validation_test_split_ddf_on_users(
        ratings_ddf,
        test_ratio=test_ratio,
        validation_ratio=validation_ratio
    )

    users = ratings_ddf['userId'].unique().compute()

    for user_id in users:
        user_total = ratings_ddf[ratings_ddf['userId'] == user_id].shape[0].compute()

        if user_total > 0:
            # Получаем данные пользователя в каждой выборке
            train_user = train[train['userId'] == user_id].shape[0].compute()
            val_user = validation[validation['userId'] == user_id].shape[0].compute() if validation_ratio > 0 else 0
            test_user = test[test['userId'] == user_id].shape[0].compute() if test_ratio > 0 else 0

            # Проверяем сохранение данных пользователя
            assert train_user + val_user + test_user == user_total, \
                f"Потеря данных для пользователя {user_id}"

            # Проверяем пропорции (допуск из-за округления до целых)
            if user_total > 1:
                expected_train = max(1, int(user_total * (1 - test_ratio - validation_ratio)))
                assert abs(train_user - expected_train) <= 1, \
                    f"Некорректное train разбиение для пользователя {user_id}"

@pytest.mark.train_test_split_ddf_per_user_test()
def test_reproducibility(ratings_ddf):
    """
    Тест воспроизводимости результатов
    """
    # Первый запуск
    train1, val1, test1 = train_validation_test_split_ddf_on_users(
        ratings_ddf,
        test_ratio=0.1,
        validation_ratio=0.1
    )

    # Второй запуск с теми же данными
    train2, val2, test2 = train_validation_test_split_ddf_on_users(
        ratings_ddf,
        test_ratio=0.1,
        validation_ratio=0.1
    )

    # Проверяем идентичность результатов
    pd.testing.assert_frame_equal(train1.compute().sort_values(['userId', 'timestamp']).reset_index(drop=True),
                                  train2.compute().sort_values(['userId', 'timestamp']).reset_index(drop=True))
    pd.testing.assert_frame_equal(val1.compute().sort_values(['userId', 'timestamp']).reset_index(drop=True),
                                  val2.compute().sort_values(['userId', 'timestamp']).reset_index(drop=True))
    pd.testing.assert_frame_equal(test1.compute().sort_values(['userId', 'timestamp']).reset_index(drop=True),
                                  test2.compute().sort_values(['userId', 'timestamp']).reset_index(drop=True))

class FakeResponse:
    def __init__(self, status_code=200, content=b""):
        self.status_code = status_code
        self.content = content



def test_download_and_extract(tmp_path, fake_zip_bytes, monkeypatch):
    def fake_get(url):
        return FakeResponse(200, fake_zip_bytes)

    monkeypatch.setattr(
        "src.data_utils.preparing_data.requests.get",
        fake_get
    )

    download_csv(str(tmp_path), "http://fake-url")

    # zip удалён
    assert not (tmp_path / "dataset.zip").exists()

    # csv распакованы
    csv_files = list(tmp_path.glob("*.csv"))
    assert len(csv_files) == 6


def test_skip_download_if_csv_exists(tmp_path, monkeypatch):
    for i in range(6):
        (tmp_path / f"file_{i}.csv").write_text("test")

    def fake_get(url):
        pytest.fail("requests.get should not be called")

    monkeypatch.setattr(
        "src.data_utils.preparing_data.requests.get",
        fake_get
    )

    download_csv(str(tmp_path), "http://fake-url")

def test_extract_existing_zip(tmp_path, fake_zip_bytes, monkeypatch):
    (tmp_path / "dataset.zip").write_bytes(fake_zip_bytes)

    def fake_get(url):
        pytest.fail("requests.get should not be called")

    monkeypatch.setattr(
        requests,
        "get",
        fake_get
    )

    download_csv(str(tmp_path), "http://fake-url")

    assert len(list(tmp_path.glob("*.csv"))) == 6
    assert not (tmp_path / "dataset.zip").exists()


def test_http_error(tmp_path, monkeypatch):
    def fake_get(url):
        return FakeResponse(404, b"")

    monkeypatch.setattr(
        requests,
        "get",
        fake_get
    )

    download_csv(str(tmp_path), "http://fake-url")

    assert not (tmp_path / "dataset.zip").exists()
    assert len(list(tmp_path.glob("*.csv"))) == 0
