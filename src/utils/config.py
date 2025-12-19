from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel


DATA_URL = Path(__file__).parent.parent.parent / "data"
ARTIFACTS_URL = Path(__file__).parent.parent.parent / "artifacts"
METRICS_URL = ARTIFACTS_URL / "metrics"

class ColumnNames(BaseModel):
    movieId: str = "movieId"
    userId: str = "userId"
    rating: str = "rating"
    timestamp: str = "timestamp"


class PreparingDataConfig(BaseModel):
    dataset_url: str = "https://www.kaggle.com/api/v1/datasets/download/grouplens/movielens-20m-dataset"
    input_folder_path: Path = DATA_URL / "raw"

    path_to_movie_csv: Path = DATA_URL / "raw" / "movie.csv"
    path_to_rating_csv: Path = DATA_URL / "raw" / "rating.csv"

    test_size: float = 0.1
    validation_size: float = 0.1

    column_names: ColumnNames = ColumnNames()


class DataConfig(BaseModel):
    split_dir: Path = ARTIFACTS_URL / "split"
    train_parquet: Path = ARTIFACTS_URL / "split" / "train.parquet"
    validation_parquet: Path = ARTIFACTS_URL / "split" / "val.parquet"
    test_parquet: Path = ARTIFACTS_URL / "split" / "test.parquet"
    models_dir: Path = ARTIFACTS_URL / "models"
    model_svd_name: str = "svd"
    model_item_based_name: str = "item-based"
    model_svd_pkl: Path = ARTIFACTS_URL / "models" / "svd.pkl"
    model_item_based_pkl: Path = ARTIFACTS_URL / "models" / "item_based.pkl"
    metrics_dir: ClassVar[Path] = Path(METRICS_URL)

    svd_best_params_for_rmse: Path = METRICS_URL / "svd_best_params_for_rmse.csv"
    svd_best_params_for_mae: Path = METRICS_URL / "svd_best_params_for_mae.csv"
    svd_metrics_path: Path = METRICS_URL / "svd_metrics.csv"
    svd_rating_metrics_path: Path = METRICS_URL / "svd_rating_metrics.csv"

    item_based_best_params_for_rmse: Path = METRICS_URL / "svd_best_params_for_rmse.csv"
    item_based_best_params_for_mae: Path = METRICS_URL / "svd_best_params_for_mae.csv"
    item_based_metrics_path: Path = METRICS_URL / "item_based_metrics.csv"
    item_based_rating_metrics_path: Path = METRICS_URL / "item_based_rating_metrics.csv"


class MetricsConfig(BaseModel):
    max_mae: list = [0.1, 0.3, 0.5, 0.7, 1.0]
    min_relevant_movies: int = 50
    n_users: int = 1000
    k: list = [5, 15, 25, 50]
    threshold_for_binarize: float = 3.5


class Settings(BaseModel):
    data: PreparingDataConfig = PreparingDataConfig()
    ml: DataConfig = DataConfig()
    metrics: MetricsConfig = MetricsConfig()


settings = Settings()