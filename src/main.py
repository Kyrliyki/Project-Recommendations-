from preparing_data import (
    load_df,
    train_test_split_df,
    save_df_to_csv,
    get_user_movie_df,
)
from config import settings
import dask.dataframe as dd


if __name__ == "__main__":

    # предобработка данных
    df = load_df(
        path_to_rating_csv=settings.data.path_to_rating_csv,
    )

    train, test = train_test_split_df(df)

    save_df_to_csv(
        train,
        settings.data.csv_save_train_path,
    )
    save_df_to_csv(
        test,
        settings.data.csv_save_test_path,
    )

    user_movie_train_df = get_user_movie_df(
        dd.read_csv(settings.data.csv_save_train_path)
    )
    user_movie_test_df = get_user_movie_df(
        dd.read_csv(settings.data.csv_save_test_path)
    )
