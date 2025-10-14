from preparing_data import (
    load_df,
    train_test_split_df,
    get_user_movie_df,
)
from config import settings


if __name__ == "__main__":

    # предобработка данных
    df = load_df(
        path_to_movie_csv=settings.data.path_to_movie_csv,
        path_to_rating_csv=settings.data.path_to_rating_csv,
    )

    train, test = train_test_split_df(df)

    user_movie_train_df = get_user_movie_df(train)
    user_movie_train_df.to_csv(
        settings.data.csv_save_train_path,
        index=False,
        single_file=True
    )
    del user_movie_train_df

    user_movie_test_df = get_user_movie_df(test)
    user_movie_test_df.to_csv(
        settings.data.csv_save_test_path,
        index=False,
        single_file=True
    )
    del user_movie_test_df
