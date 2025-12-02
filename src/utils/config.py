from pathlib import Path
from pydantic import BaseModel


BASE_URL = Path(__file__).parent.parent.parent / "data"


class ColumnNames(BaseModel):
    movieId: str = "movieId"
    userId: str = "userId"
    rating: str = "rating"
    timestamp: str = "timestamp"


class PreparingDataConfig(BaseModel):
    dataset_url: str = "https://www.kaggle.com/api/v1/datasets/download/grouplens/movielens-20m-dataset"
    input_folder_path: Path = BASE_URL / "raw"

    path_to_movie_csv: Path = BASE_URL / "raw" / "movie.csv"
    path_to_rating_csv: Path = BASE_URL / "raw" / "rating.csv"

    test_size: float = 0.1
    validation_size: float = 0.1

    column_names: ColumnNames = ColumnNames()


class DataConfig(BaseModel):
    train_parquet: Path = BASE_URL / "split" / "train.parquet"
    validation_parquet: Path = BASE_URL / "split" / "validation.parquet"
    test_parquet: Path = BASE_URL / "split" / "test.parquet"

    model_svd_pkl: Path = BASE_URL / "models" / "svd.pkl"
    model_item_based_pkl: Path = BASE_URL / "models" / "item_based.pkl"

    svd_metrics_path: Path = BASE_URL / "models" / "svd_metrics.csv"
    svd_rating_metrics_path: Path = BASE_URL / "models" / "svd_rating_metrics.csv"
    item_based_metrics_path: Path = BASE_URL / "models" / "item_based_metrics.csv"
    item_based_rating_metrics_path: Path = BASE_URL / "models" / "item_based_rating_metrics.csv"


class MetricsConfig(BaseModel):
    max_mae: float = 0.5
    min_relevant_movies: int = 50
    n_users: int = 1000
    k: list = [5, 10, 15, 20, 25, 50]
    threshold_for_binarize: float = 3.5


class Settings(BaseModel):
    data: PreparingDataConfig = PreparingDataConfig()
    ml: DataConfig = DataConfig()
    metrics: MetricsConfig = MetricsConfig()


settings = Settings()