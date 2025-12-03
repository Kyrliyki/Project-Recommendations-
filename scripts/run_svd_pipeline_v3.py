import dask.dataframe as dd
import time
import pickle

from tqdm import tqdm

from src.utils.config import settings
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.data_utils.preparing_data import (download_csv, train_validation_test_split_ddf)
from src.pipelines.metric_pipeline import MetricPipeline
import random



def main():
    start_time = time.time()

    # print("\nЗагрузка датасета...")
    # download_csv(
    #      input_folder_path=settings.data.input_folder_path,
    #      url=settings.data.dataset_url
    # )
    # print("Датасет загружен!")

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
    train_users = train[settings.data.column_names.userId].compute().unique().tolist()

    print(f"Найдено {len(val_users_with_relevant)} пользователей в validation с релевантными оценками.")
    selected_users = [user for user in val_users_with_relevant if user in train_users]

    selected_users = random.sample(selected_users, settings.metrics.n_users)
    print(f"Выбрано {len(selected_users)} пользователей для оценки.")


    all_movie_ids = set(train[settings.data.column_names.movieId].compute().unique().tolist())
    all_movie_ids.update(validation[settings.data.column_names.movieId].compute().unique().tolist())

    print("Собираем релевантные фильмы из validation для выбранных пользователей...")

    all_recommendations = []
    all_relevant = []
    print("Запуск оценки по протоколу 1:99 (один релевантный + 99 негативов)...")

    for user in tqdm(selected_users, desc="Пользователи", unit="item"):
        relevant_movies = filtered_val[
            filtered_val[settings.data.column_names.userId] == user
        ][settings.data.column_names.movieId].compute().tolist()

        if len(relevant_movies) == 0:
            print(f"Пользователь {user} не имеет релевантных фильмов в validation. Пропускаем.")
            continue

        user_rated_in_train = train[train[settings.data.column_names.userId] == user][
            settings.data.column_names.movieId
        ].compute().tolist()
        user_rated_in_val = validation[validation[settings.data.column_names.userId] == user][
            settings.data.column_names.movieId
        ].compute().tolist()
        user_seen_movies = set(user_rated_in_train + user_rated_in_val)

        negative_candidates = list(all_movie_ids - user_seen_movies)

        n_negatives_needed = len(relevant_movies) * 99

        if len(negative_candidates) < n_negatives_needed:
            print(f"Предупреждение: недостаточно негативных кандидатов для пользователя {user}. "
                  f"Требуется {n_negatives_needed}, доступно {len(negative_candidates)}. Пропускаем пользователя.")
            continue

        negative_sample = random.sample(negative_candidates, n_negatives_needed)
        candidate_set = relevant_movies + negative_sample
        random.shuffle(candidate_set)

        recommendations = model.getting_recommended_movies(
            user_id=user,
            movies_list=candidate_set
        )

        all_recommendations.append(recommendations)
        all_relevant.append(relevant_movies)

    print(f"Всего проведено {len(all_recommendations)} оценочных раундов (по числу релевантных фильмов).")
    print("Подсчитываем метрики...")

    metric_pipeline = MetricPipeline(
        k_list=settings.metrics.k,
        metrics=["Precision", "Recall", "MAP", "NDCG"]
    )

    results_df = metric_pipeline.run(
        model_recommendations={"SVD_v3": all_recommendations},
        relevant_items=all_relevant,
    )

    results_df.to_csv('data/models/svd_v3_metrics.csv')

    print("\nРезультаты метрик:")
    print(results_df)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nВремя выполнения: {duration:.6f} секунд")

if __name__=="__main__":
    main()
