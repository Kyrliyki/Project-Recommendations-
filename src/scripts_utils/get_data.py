import dask.dataframe as dd

from src.data_utils.preparing_data import (
    download_csv,
    train_validation_test_split_ddf_on_users,
)
from src.utils.config import settings


def get_data():
    print("\nЗагрузка датасета...")
    download_csv(
        input_folder_path=settings.data.input_folder_path,
        url=settings.data.dataset_url
    )
    print("Датасет загружен!")

    print("\nПроверка наличия сохранённых данных...")
    train_parquet = settings.ml.train_parquet
    validation_parquet = settings.ml.validation_parquet
    test_parquet = settings.ml.test_parquet
    if all(path.exists() for path in [
        train_parquet,
        validation_parquet,
        test_parquet,
    ]):
        print("Сохранённые данные найдены. Загружаем из Parquet...")
        train = dd.read_parquet(train_parquet)
        validation = dd.read_parquet(validation_parquet)
        test = dd.read_parquet(test_parquet)
        print("Данные загружены из Parquet!")
    else:
        print("Сохранённых данных не найдено. Загружаем исходный датасет и разделяем...")
        download_csv(
            input_folder_path=settings.data.input_folder_path,
            url=settings.data.dataset_url
        )
        print("Датасет загружен!")

        print("\nДеление на train, validation, test...")
        df = dd.read_csv(
            settings.data.path_to_rating_csv,
            parse_dates=[settings.data.column_names.timestamp],
        )
        train, validation, test = train_validation_test_split_ddf_on_users(df)
        print("Датасет поделен на train, validation, test выборки!")

        print("Сохраняем данные в Parquet...")
        train.to_parquet(train_parquet)
        validation.to_parquet(validation_parquet)
        test.to_parquet(test_parquet)
        print("Данные сохранены в Parquet!")

    return train, validation, test