import dask.dataframe as dd
import time
import pickle

from tqdm import tqdm

from src.utils.config import settings
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.preparing_data import (
    download_csv,
    train_validation_test_split_ddf,
)
from src.pipelines.metric_pipeline import MetricPipeline


def main():
    start_time = time.time()

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
        train, validation, test = train_validation_test_split_ddf(df)
        print("Датасет поделен на train, validation, test выборки!")

        print("Сохраняем данные в Parquet...")
        train.to_parquet(train_parquet)
        validation.to_parquet(validation_parquet)
        test.to_parquet(test_parquet)
        print("Данные сохранены в Parquet!")

    print("\nПроверка наличия сохранённой модели...")
    model_pkl = settings.ml.model_svd_pkl
    if model_pkl.exists():
        print("Сохранённая модель найдена. Загружаем...")
        try:
            with model_pkl.open("rb") as f:
                model = pickle.load(f)
            print("Модель загружена из", model_pkl)
        except Exception as e:
            print(f"Ошибка при загрузке модели: {e}. Обучаем заново...")
            model = MLMatrixFactorizationSVD()
            model.fit(train)
            print("Модель обучена!")

            try:
                with model_pkl.open("wb") as f:
                    pickle.dump(model, f)
                print("Модель сохранена в", model_pkl)
            except Exception as e:
                print(f"Не удалось сохранить модель: {e}")
    else:
        print("Сохранённой модели не найдено. Обучаем...")
        model = MLMatrixFactorizationSVD()
        model.fit(train)
        print("Модель обучена!")

        try:
            with model_pkl.open("wb") as f:
                pickle.dump(model, f)
            print("Модель сохранена в", model_pkl)
        except Exception as e:
            print(f"Не удалось сохранить модель: {e}")

    print("\nТестирование модели...")

    filtered_val = validation[
        validation[settings.data.column_names.rating] >= settings.metrics.threshold_for_binarize
        ]
    val_users_with_relevant = filtered_val[settings.data.column_names.userId].compute().unique().tolist()

    if len(val_users_with_relevant) == 0:
        raise ValueError("В validation нет пользователей с оценками выше порога!")
    print(f"Найдено {len(val_users_with_relevant)} пользователей в validation с релевантными оценками.")

    selected_users = val_users_with_relevant[:settings.metrics.n_users]
    print(f"Выбрано {len(selected_users)} пользователей для оценки.")

    print("Собираем релевантные фильмы из validation для выбранных пользователей и составляем рекомендации...")
    all_recommendations = []
    all_relevant = []
    for user in tqdm(selected_users, desc="Пользователи", unit="item"):
        relevant_movies = filtered_val[filtered_val[settings.data.column_names.userId] == user][
            settings.data.column_names.movieId].compute().unique().tolist()
        all_movies_list_for_current_user = validation[validation[settings.data.column_names.userId] == user][
            settings.data.column_names.movieId].compute().unique().tolist()
        recommendations = model.getting_recommended_movies(
            user_id=user,
            movies_list=all_movies_list_for_current_user,
        )
        all_recommendations.append(recommendations)
        all_relevant.append(relevant_movies)

    print("Подсчитываем метрики...")
    metric_pipeline = MetricPipeline(
        k_list=settings.metrics.k,  # можно несколько K
        metrics=["Precision", "Recall", "MAP", "NDCG"]
    )
    results_df = metric_pipeline.run(
        model_recommendations={model.model_name: all_recommendations},
        relevant_items=all_relevant,
    )

    results_df.to_csv(settings.ml.svd_metrics_path, index=False)

    print("\nРезультаты метрик:")
    print(results_df)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nВремя выполнения: {duration:.6f} секунд")


if __name__=="__main__":
    main()
