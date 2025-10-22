from pydantic import BaseModel

class ColumnNames(BaseModel):
    movieId: str = "movieId"
    userId: str = "userId"
    rating: str = "rating"
    timestamp: str = "timestamp"


class PreparingDataConfig(BaseModel):
    path_to_movie_csv: str = "data_csv/input/movie.csv"
    path_to_rating_csv: str = "data_csv/input/rating.csv"

    csv_save_train_path: str = "data_csv/output/train/*.csv"
    csv_save_test_path: str = "data_csv/output/test/*.csv"

    dataset_url: str = "https://www.kaggle.com/api/v1/datasets/download/grouplens/movielens-20m-dataset"
    input_folder_path: str = "data_csv/input"

    test_size: float = 0.1
    validation_size: float = 0.1
    random_state: int = 42
    shuffle: bool = False

    column_names: ColumnNames = ColumnNames()


class Settings(BaseModel):
    data: PreparingDataConfig = PreparingDataConfig()


settings = Settings()