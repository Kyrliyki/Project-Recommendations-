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

    print(train.npartitions)

    user_movie_train_df = get_user_movie_df(train)
    user_movie_test_df = get_user_movie_df(test)
