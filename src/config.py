from pydantic import BaseModel


class ColumnNames(BaseModel):
    movieId = "movieId"
    userId = "userId"
    rating = "rating"
    timestamp = "timestamp"


class PreparingDataConfig(BaseModel):
    path_to_movie_csv: str = "data_csv/input/movie.csv"
    path_to_rating_csv: str = "data_csv/input/rating.csv"

    csv_save_train_path: str = "data_csv/output/train/*.csv"
    csv_save_test_path: str = "data_csv/output/test/*.csv"

    test_size: float = 0.2
    random_state: int = 42
    shuffle: bool = True

    column_names: ColumnNames = ColumnNames()


class Settings(BaseModel):
    data: PreparingDataConfig = PreparingDataConfig()


settings = Settings()