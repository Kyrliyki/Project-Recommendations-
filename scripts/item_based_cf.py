import time

from src.pipelines.metric_for_predicted_estimates_pipeline import MetricForPredictedEstimatesPipeline
from src.utils.config import settings

from tqdm import tqdm

from src.ml_models.item_based_cf.model import MLItemBasedCFSimple
from src.pipelines.metric_pipeline import MetricPipeline
from src.scripts_utils.get_data import get_data
from src.scripts_utils.get_model import get_model


def main():
    start_time = time.time()

    train, validation, test = get_data()
    model = get_model(
        model_cls=MLItemBasedCFSimple,
        model_pkl=settings.ml.model_item_based_pkl,
        train=train,
    )

    print("\nТестирование модели...")

    filtered_val = validation[
        validation[settings.data.column_names.rating] >= settings.metrics.threshold_for_binarize
        ]
    # сортировка пользователей по встречаемости в выборке (по возрастанию)
    val_users_with_relevant_value_counts = (
        filtered_val[settings.data.column_names.userId]
        .compute()
        .value_counts(ascending=True)
    )

    if len(val_users_with_relevant_value_counts) == 0:
        raise ValueError("В validation нет пользователей с оценками выше порога!")
    print(f"Найдено {len(val_users_with_relevant_value_counts)} пользователей в validation с релевантными оценками.")

    # выбор пользователей с минимальным количеством оцененных фильмов = settings.metrics.min_relevant_movies
    val_users_with_relevant = val_users_with_relevant_value_counts[
        val_users_with_relevant_value_counts >= settings.metrics.min_relevant_movies
        ]

    # выбор settings.metrics.n_users пользователей
    selected_users_with_value_counts = val_users_with_relevant.head(
        settings.metrics.n_users
    )
    selected_users = selected_users_with_value_counts.index
    print(f"Выбрано {len(selected_users)} пользователей в validation, у которых "
          f"от {selected_users_with_value_counts.min()} "
          f"до {selected_users_with_value_counts.max()} "
          f"положительно оцененных фильмов.")

    print("Собираем релевантные фильмы из validation для выбранных пользователей и составляем рекомендации...")
    all_recommendations = []
    all_relevant = []
    all_y_true = []
    all_y_predicted = []
    for user in tqdm(selected_users, desc="Пользователи", unit="item"):
        validation_for_current_user = validation[validation[settings.data.column_names.userId] == user]

        relevant_movies = filtered_val[filtered_val[settings.data.column_names.userId] == user][
            settings.data.column_names.movieId].compute().unique().tolist()
        all_movies_list_for_current_user = validation_for_current_user[
            settings.data.column_names.movieId].compute().unique().tolist()
        recommendations = model.getting_recommended_movies(
            user_id=user,
            movies_list=all_movies_list_for_current_user,
        )
        all_recommendations.append(recommendations)
        all_relevant.append(relevant_movies)

        y_true = validation_for_current_user[settings.data.column_names.rating].compute().tolist()
        y_predicted = []
        for index, row in validation_for_current_user.iterrows():
            rating = model.predict(
                user_id=user,
                movie_id=row[settings.data.column_names.movieId],
            )
            y_predicted.append(rating.est)
        all_y_true.append(y_true)
        all_y_predicted.append(y_predicted)

    print("Подсчитываем метрики...")
    metric_pipeline = MetricPipeline(
        k_list=settings.metrics.k,  # можно несколько K
        metrics=["Precision", "Recall", "MAP", "NDCG"]
    )
    results_df = metric_pipeline.run(
        model_recommendations={model.model_name: all_recommendations},
        relevant_items=all_relevant,
    )
    results_df.to_csv(settings.ml.item_based_metrics_path, index=False)

    k_list = settings.metrics.k
    k_list.append(None)
    metric_for_predicted_estimates_pipeline = MetricForPredictedEstimatesPipeline(
        k_list=k_list,
        max_mae=settings.metrics.max_mae,
        metrics=["Accuracy", "Precision", "Recall"],
    )
    results_predicted_est_df = metric_for_predicted_estimates_pipeline.run(
        model_name=model.model_name,
        y_true=all_y_true,
        y_predicted=all_y_predicted,
    )
    results_predicted_est_df.to_csv(settings.ml.item_based_rating_metrics_path, index=False)

    print("\nРезультаты метрик:")
    print(results_df)
    print(results_predicted_est_df)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nВремя выполнения: {duration:.6f} секунд")


if __name__=="__main__":
    main()