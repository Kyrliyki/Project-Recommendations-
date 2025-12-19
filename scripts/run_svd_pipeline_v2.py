import time

from src.data_utils.data_loader import DataLoader
from src.evaluation.protocols.one_to_many import OnePositiveToManyNegativesProtocol
from src.pipelines.svd_pipeline import SVDPipeline
from src.utils.config import settings
from src.pipelines.metric_pipeline import MetricPipeline




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

    protocol = OnePositiveToManyNegativesProtocol(
        n_users=settings.metrics.n_users,
        threshold=settings.metrics.threshold_for_binarize,
    )

    test_cases = protocol.prepare_test_cases(train, validation)

    all_recommendations, all_relevant = model.collect_recommendations(test_cases)

    metric_pipeline = MetricPipeline(
        k_list=settings.metrics.k,
        metrics=["Precision", "Recall", "MAP", "NDCG"]
    )

    results_df = metric_pipeline.run(
        model_recommendations={"SVD_v2": all_recommendations},
        relevant_items=all_relevant,
    )

    results_df.to_csv(settings.ml.metrics_dir / 'svd_v2_metrics.csv',  index=False)

    print("\nРезультаты метрик:")
    print(results_df)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\nВремя выполнения: {duration:.6f} секунд")

if __name__=="__main__":
    main()
