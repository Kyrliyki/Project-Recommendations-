import pytest

from src.preparing_data import train_test_split_ddf









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

    train, validation, test = train_test_split_ddf(
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

    train_max = train['timestamp'].max().compute()
    val_min = validation['timestamp'].min().compute()
    val_max = validation['timestamp'].max().compute()
    test_min = test['timestamp'].min().compute()

    assert train_max <= val_min, f"Train max {train_max} > Validation min {val_min}"
    assert val_max <= test_min, f"Validation max {val_max} > Test min {test_min}"

    def check_timestamps_sorted(df, set_name):
        """Проверяем, что timestamp'ы отсортированы по возрастанию"""
        timestamps = df['timestamp'].compute().values
        is_sorted = all(timestamps[i] <= timestamps[i + 1] for i in range(len(timestamps) - 1))
        assert is_sorted, f"Timestamps in {set_name} are not sorted chronologically"

    def check_no_cross_timeline_leakage(train, validation, test):
        """Проверяем, что нет записей из 'будущего' в более ранних выборках"""
        val_min = validation['timestamp'].min().compute()
        test_min = test['timestamp'].min().compute()


        train_late_records = train[train['timestamp'] > val_min].compute()
        assert len(
            train_late_records) == 0, f"Found {len(train_late_records)} records in train that are later than validation start"

        val_late_records = validation[validation['timestamp'] > test_min].compute()
        assert len(
            val_late_records) == 0, f"Found {len(val_late_records)} records in validation that are later than test start"

    check_no_cross_timeline_leakage(train, validation, test)

    check_timestamps_sorted(train, "train")
    check_timestamps_sorted(validation, "validation")
    check_timestamps_sorted(test, "test")