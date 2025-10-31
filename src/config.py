from pydantic import BaseModel

class ColumnNames(BaseModel):
    movieId: str = "movieId"
    userId: str = "userId"
    rating: str = "rating"
    timestamp: str = "timestamp"


class PreparingDataConfig(BaseModel):
    path_to_movie_csv: str = "data_csv/input/movie.csv"
    path_to_rating_csv: str = "data_csv/input/rating.csv"

    dataset_url: str = "https://www.kaggle.com/api/v1/datasets/download/grouplens/movielens-20m-dataset"
    input_folder_path: str = "data_csv/input"

    test_size: float = 0.1
    validation_size: float = 0.1

    column_names: ColumnNames = ColumnNames()


class MetricsConfig(BaseModel):
    k: int = 10


class Settings(BaseModel):
    data: PreparingDataConfig = PreparingDataConfig()
    metrics: MetricsConfig = MetricsConfig()


settings = Settings()