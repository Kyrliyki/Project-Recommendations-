import time

from tqdm import tqdm

from src.data_utils.data_loader import DataLoader
from src.evaluation.protocols.only_all_relevant import OnlyAllPositives
from src.pipelines.metric_for_predicted_estimates_pipeline import MetricForPredictedEstimatesPipeline
from src.pipelines.svd_pipeline import SVDPipeline
from src.utils.config import settings
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.pipelines.metric_pipeline import MetricPipeline
from src.scripts_utils.get_data import get_data
from src.scripts_utils.get_model import get_model


def main():
    start_time = time.time()

    data_loader = DataLoader(
        raw_csv_path=settings.data.input_folder_path,
        splits_dir=settings.ml.split_dir,
        split_strategy='user',
        split_strategy_kwargs={
            "test_ratio": settings.data.test_size,
            "validation_ratio": settings.data.validation_size,
        },
        download_url=settings.data.dataset_url,
    )

    print("\nТестирование модели...")

    train, validation, _ = data_loader.load()


    model = SVDPipeline(
        model_name=settings.ml.model_svd_name,
        models_dir=settings.ml.models_dir,
    )

    model.train(train)

    protocol = OnlyAllPositives(
        n_users=settings.metrics.n_users,
        threshold=settings.metrics.threshold_for_binarize,
        min_relevant_items=settings.metrics.min_relevant_movies,
    )

    test_cases = protocol.prepare_test_cases(train, validation)

    all_recommendations, all_relevant = model.collect_recommendations(test_cases)

    all_y_true, all_y_predicted = protocol.collect_rating_predictions(
        model_pipeline=model,
        test_cases=test_cases,
        validation=validation
    )

    metric_pipeline = MetricPipeline(
        k_list=settings.metrics.k,
        metrics=["Precision", "Recall", "MAP", "NDCG"]
    )
    results_df = metric_pipeline.run(
        model_recommendations={model.model_name: all_recommendations},
        relevant_items=all_relevant,
    )
    results_df.to_csv(settings.ml.svd_metrics_path, index=False)

    k_list = settings.metrics.k
    k_list.append(None)
    metric_for_predicted_estimates_pipeline = MetricForPredictedEstimatesPipeline(
        k_list=k_list,
        max_mae=settings.metrics.max_mae,
        metrics=["Accuracy", "Precision"],
    )
    results_predicted_est_df = metric_for_predicted_estimates_pipeline.run(
        model_name=model.model_name,
        y_true=all_y_true,
        y_predicted=all_y_predicted,
    )
    results_predicted_est_df.to_csv(settings.ml.svd_rating_metrics_path, index=False)

    print("\nРезультаты метрик:")
    print(results_df)
    print(results_predicted_est_df)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nВремя выполнения: {duration:.6f} секунд")


if __name__=="__main__":
    main()
