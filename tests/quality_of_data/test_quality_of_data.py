import dask
import numpy as np
import pandas as pd

from src.data_utils.preparing_data import train_validation_test_split_ddf_on_users




def test_user_timeline_consistency(real_ratings_ddf):
    """Проверка утечек времени для пользователей"""

    train, validation, test = train_validation_test_split_ddf_on_users(
        real_ratings_ddf, test_ratio=0.1, validation_ratio=0.1
    )

    user_stats = dask.compute(
        train.groupby('userId')['timestamp'].max().rename('train_max'),
        validation.groupby('userId')['timestamp'].min().rename('val_min'),
        validation.groupby('userId')['timestamp'].max().rename('val_max'),
        test.groupby('userId')['timestamp'].min().rename('test_min'),
        train.groupby('userId')['timestamp'].count().rename('train_count'),
        validation.groupby('userId')['timestamp'].count().rename('val_count'),
        test.groupby('userId')['timestamp'].count().rename('test_count')
    )

    stats_df = pd.concat(user_stats, axis=1)
    mask_train_val = (stats_df['train_count'] > 0) & (stats_df['val_count'] > 0)

    if mask_train_val.any():
        violations_train_val = stats_df.loc[
            mask_train_val & (stats_df['train_max'] > stats_df['val_min'])
            ]
        assert len(violations_train_val) == 0, (
            f"Нарушение порядка train-validation для {len(violations_train_val)} пользователей: "
            f"{violations_train_val.index.tolist()[:10]}"
        )


    mask_val_test = (stats_df['val_count'] > 0) & (stats_df['test_count'] > 0)
    if mask_val_test.any():
        violations_val_test = stats_df.loc[
            mask_val_test & (stats_df['val_max'] > stats_df['test_min'])
            ]
        assert len(violations_val_test) == 0, (
            f"Нарушение порядка validation-test для {len(violations_val_test)} пользователей: "
            f"{violations_val_test.index.tolist()[:10]}"
        )


    def check_sorted_per_user_vectorized(df, name):
        """Векторизованная проверка сортировки"""
        user_min_max = df.groupby('userId')['timestamp'].agg(['min', 'max']).compute()


        if (user_min_max['min'] > user_min_max['max']).any():
            problem_users = user_min_max[user_min_max['min'] > user_min_max['max']].index

            for user_id in problem_users[:10]:  # Проверяем только первых 10
                user_times = df[df['userId'] == user_id]['timestamp'].compute().values
                if len(user_times) > 1:
                    is_sorted = np.all(user_times[:-1] <= user_times[1:])
                    assert is_sorted, f"Timestamps не отсортированы для пользователя {user_id} в {name}"

    check_sorted_per_user_vectorized(train, "train")
    check_sorted_per_user_vectorized(validation, "validation")
    check_sorted_per_user_vectorized(test, "test")

def test_no_missing_values(real_ratings_ddf, real_movies_ddf):

    null_count_ratings, null_count_movies = dask.compute(
        real_ratings_ddf.isnull().sum().sum(),
        real_movies_ddf.isnull().sum().sum()
    )
    assert null_count_ratings == 0, \
        f"Найдены пропущенные значения в ratings: {null_count_ratings}"

    assert null_count_movies == 0, \
        f"Найдены пропущенные значения в movies: {null_count_movies}"


def test_unique_user_movie_pairs(real_ratings_ddf):
    """(userId, movieId) должна быть уникальной в ratings"""

    grouped = real_ratings_ddf.groupby(['userId', 'movieId']).size()
    has_duplicates = (grouped > 1).any().compute()
    assert not has_duplicates, "Найдены дубликаты user-movie пар в ratings"

def test_movieId_in_movies_exists_in_ratings(real_ratings_ddf, real_movies_ddf):
    """Все movieId из ratings должны быть в movies"""
    movie_ids_ratings = real_ratings_ddf['movieId'].unique()
    movie_ids_movies = real_movies_ddf['movieId'].unique()


    anti_join = movie_ids_ratings.map_partitions(
        lambda x: x[~x.isin(movie_ids_movies.compute())]
    )
    missing = anti_join.compute()

    assert len(missing) == 0, f"В ratings есть movieId, отсутствующие в movies: {len(missing)} шт."

def test_consistency(real_ratings_ddf):
    """userId и movieId должны быть положительными"""

    min_user_id, min_movie_id = dask.compute(
        real_ratings_ddf['userId'].min(),
        real_ratings_ddf['movieId'].min()
    )
    assert min_user_id >= 0, "userId должны быть >= 0"
    assert min_movie_id >= 0, "movieId должны быть >= 0"

def test_rating_range(real_ratings_ddf):
    """Рейтинг должен быть от 0.5 до 5"""
    invalid = real_ratings_ddf['rating'].map_partitions(
        lambda part: (part < 0.5) | (part > 5.0)
    ).any().compute()

    assert not invalid, "Некорректные рейтинги обнаружены: значение вне диапазона [0.5, 5.0]"
