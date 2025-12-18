from pathlib import Path

import dask.dataframe as dd
import time

from src.data_utils.data_loader import DataLoader
from src.evaluation.protocols.all_to_many import AllPositivesToManyNegativesProtocol
from src.evaluation.protocols.one_to_many import OnePositiveToManyNegativesProtocol
from src.evaluation.protocols.only_all_relevant import OnlyAllPositives
from src.pipelines.metric_for_predicted_estimates_pipeline import MetricForPredictedEstimatesPipeline
from src.pipelines.svd_pipeline import SVDPipeline
from src.utils.config import settings
from src.ml_models.matrix_factorization_based_cf.model import MLMatrixFactorizationSVD
from src.data_utils.preparing_data import (download_csv, train_validation_test_split_ddf)
from src.pipelines.metric_pipeline import MetricPipeline

start_time = time.time()
#BASE_URL = Path(__file__).parent.parent / "testing"
print("\nЗагрузка датасета...")
data_loader = DataLoader(
    raw_csv_path="testing/data/raw",
    splits_dir="testing/artifacts/splits",
    split_strategy='user',
    split_strategy_kwargs={
        "test_ratio": settings.data.test_size,
        "validation_ratio": settings.data.validation_size,
    },
    download_url=settings.data.dataset_url,
)
print("Датасет загружен!")

print("\nДеление на train, validation, test...")
train, validation, _ = data_loader.load()
print("Датасет поделен на train, validation, test выборки!")

train_head = train.loc[:1000]
validation_head = validation.loc[:1000]

model = SVDPipeline(
    model_name='SVD',
    models_dir="testing/artifacts/models"
)

model.train(train_head)

protocol = OnlyAllPositives(
    n_users=100,
    threshold=2.0,
    min_relevant_items=1
)

test_cases = protocol.prepare_test_cases(train_head, validation_head)



all_recommendations, all_relevant = model.collect_recommendations(test_cases)

all_y_true, all_y_predicted = protocol.collect_rating_predictions(
    model_pipeline=model,
    test_cases=test_cases,
    validation=validation_head
)
print("Подсчитываем метрики...")
metric_pipeline = MetricPipeline(
    k_list=[5, 10, 20],
    metrics=["Precision", "Recall", "MAP", "NDCG"]
)
results_df = metric_pipeline.run(
    model_recommendations={model.model_name: all_recommendations},
    relevant_items=all_relevant,
)
results_df.to_csv("testing/artifacts/ml_recommendations2.1.csv", index=False)

k_list = [5, 10, 20]
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
results_predicted_est_df.to_csv("testing/artifacts/ml_recommendations2.2.csv", index=False)

print("\nРезультаты метрик:")
print(results_df)
print(results_predicted_est_df)

end_time = time.time()
duration = end_time - start_time
print(f"\nВремя выполнения: {duration:.6f} секунд")
