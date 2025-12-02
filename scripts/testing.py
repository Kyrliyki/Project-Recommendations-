import dask.dataframe as dd
import time
from src.utils.config import settings
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.data_utils.preparing_data import (download_csv, train_validation_test_split_ddf)
from src.pipelines.metric_pipeline import MetricPipeline

start_time = time.time()

print("\nЗагрузка датасета...")
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

print("\nОбучение модели...")
model = MLMatrixFactorizationSVD()
model.fit(train)
print("Модель обучена!")

print("\nТестирование модели...")

# Шаг 1. Фильтруем validation: только оценки выше порога
filtered_val = validation[
    validation[settings.data.column_names.rating] > settings.metrics.threshold_for_binarize
    ]

# Шаг 2. Находим пользователей в validation с релевантными оценками
val_users_with_relevant = (
    filtered_val[settings.data.column_names.userId]
    .drop_duplicates()
    .compute()
    .values
)

if len(val_users_with_relevant) == 0:
    raise ValueError("В validation нет пользователей с оценками выше порога!")

print(f"Найдено {len(val_users_with_relevant)} пользователей в validation с релевантными оценками.")


# Шаг 3.1. Получаем уникальных пользователей из train
train_users = set(train[settings.data.column_names.userId].compute())

# Шаг 3.2. Фильтруем: оставляем только пользователей, которые есть и в validation, и в train
common_users = [user for user in val_users_with_relevant if user in train_users]

print(f"Из них {len(common_users)} пользователей также присутствуют в train.")


if len(common_users) == 0:
    raise ValueError("Нет пользователей, которые одновременно есть в train и validation с релевантными оценками!")


# Шаг 3.3 Ограничиваем выборку до N пользователей (если нужно)
n_users = min(settings.metrics.n_users, len(common_users))
selected_users = common_users[:n_users]
print(f"Выбрано {len(selected_users)} пользователей для оценки.")

# Шаг 4. Собираем релевантные фильмы для каждого пользователя из selected_users
print("Собираем релевантные фильмы из validation для выбранных пользователей...")
relevant_map = (
    filtered_val
    .groupby(settings.data.column_names.userId)[settings.data.column_names.movieId]
    .apply(list, meta=('movieId', 'object'))
    .compute()
)

# Убедимся, что все выбранные пользователи есть в relevant_map (иначе — пустой список)
all_relevant = []
for user in selected_users:
    relevant_movies = relevant_map.get(user, [])
    all_relevant.append(relevant_movies)

# Шаг 5. Получаем рекомендации для каждого пользователя
print("Получаем рекомендации от модели...")
all_recommendations = []

# Заранее получаем множество фильмов из train (чтобы фильтровать рекомендации)
train_movie_ids = set(train[settings.data.column_names.movieId].compute())

for user in selected_users:
    # Получаем топ-K рекомендаций
    recommendations = model.getting_recommended_movies(
        user_id=user,
        top_k=10  # берём с запасом, потом отфильтруем
    )

    # Фильтруем: только фильмы из train и не из train-истории пользователя
    train_user_movies = set(
        train[train[settings.data.column_names.userId] == user][
            settings.data.column_names.movieId
        ].compute()
    )

    filtered_recs = [
                        movie for movie in recommendations
                        if (movie in train_movie_ids) and (movie not in train_user_movies)
                    ][:10]  # обрезаем до 10

    all_recommendations.append(filtered_recs)

# Шаг 6. Проверяем, что есть данные для метрик
if len(all_recommendations) == 0 or len(all_relevant) == 0:
    raise ValueError("Нет данных для расчёта метрик (рекомендации или релевантные списки пусты).")

print(f"\nРассчитываем метрики для {len(all_recommendations)} пользователей...")

# Шаг 7. Запускаем расчёт метрик
metric_pipeline = MetricPipeline(
    k_list=[5],  # можно несколько K
    metrics=["Precision", "Recall", "MAP", "NDCG"]
)

results_df = metric_pipeline.run(
    model_recommendations={"MLMatrixFactorizationSVD": all_recommendations},
    relevant_items=all_relevant
)

print("\nРезультаты метрик:")
print(results_df)

end_time = time.time()
duration = end_time - start_time
print(f"\nВремя выполнения: {duration:.6f} секунд")
