import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from src.data_utils.preparing_data import train_validation_test_split_ddf, train_validation_test_split_ddf_on_users
from src.utils.config import settings





# def test_data_structure():
#     """ Проверка наличия столбцов """
#
#     assert 'userId' in ratings.columns, "Нет столбца userId в ratings"
#     assert 'movieId' in ratings.columns, "Нет столбца movieId в ratings"
#     assert 'rating' in ratings.columns, "Нет столбца rating в ratings"
#     assert 'timestamp' in ratings.columns, "Нет столбца timestamp в ratings"
#
#     dtypes = ratings.dtypes.compute()
#     assert dtypes['userId'] in ['int64', 'int32'], "userId должен быть целым"
#     assert dtypes['movieId'] in ['int64', 'int32'], "movieId должен быть целым"
#     assert dtypes['rating'] in ['float64', 'float32'], "rating должен быть целым"
#     assert dtypes['timestamp'] in ['datetime64[ns]'], "timestamp должен быть целым"

# def test_no_temporal_leakage_global(real_ratings_ddf):
#     """Проверка утечки времени записей рейтингов"""
#     train, validation, test = train_validation_test_split_ddf(
#         real_ratings_ddf, test_ratio=0.1, validation_ratio=0.1
#     )
#
#     train_max, val_min, val_max, test_min = dask.compute(
#         train['timestamp'].max(),
#         validation['timestamp'].min(),
#         validation['timestamp'].max(),
#         test['timestamp'].min()
#     )
#
#     assert train_max <= val_min, f"Train max {train_max} > Validation min {val_min}"
#     assert val_max <= test_min, f"Validation max {val_max} > Test min {test_min}"
#
#     def check_timestamps_sorted(df, set_name):
#         """Проверяем, что timestamp'ы отсортированы по возрастанию"""
#         ts_min, ts_max = dask.compute(
#             df['timestamp'].min(),
#             df['timestamp'].max()
#         )
#
#         if ts_min > ts_max:
#             assert False, f"Timestamps in {set_name}: min {ts_min} > max {ts_max}"
#
#         # 2. Если данные уже в Dask, проверяем партиции
#         if hasattr(df, 'npartitions'):
#             # Берем только несколько партиций для проверки
#             n_to_check = min(3, df.npartitions)
#
#             for i in range(n_to_check):
#                 part = df.get_partition(i)
#                 if len(part) > 1:
#                     # Проверяем только внутри партиции
#                     part_times = part['timestamp'].compute().values
#                     if len(part_times) > 1:
#                         is_sorted = np.all(part_times[:-1] <= part_times[1:])
#                         if not is_sorted:
#                             # Если в партиции не отсортировано, проверяем все
#                             all_times = df['timestamp'].compute().values
#                             is_sorted_all = np.all(all_times[:-1] <= all_times[1:])
#                             assert is_sorted_all, f"Timestamps in {set_name} not sorted"
#                             return
#
#             # Если все проверенные партиции отсортированы, считаем что OK
#             return
#
#         # 3. Fallback: оригинальная проверка
#         timestamps = df['timestamp'].compute().values
#         is_sorted = np.all(timestamps[:-1] <= timestamps[1:])
#         assert is_sorted, f"Timestamps in {set_name} are not sorted chronologically"
#
#     def check_no_cross_timeline_leakage(train, validation, test):
#         """Проверяем, что нет записей из 'будущего' в более ранних выборках"""
#
#         train_late_count, val_late_count = dask.compute(
#             (train['timestamp'] > validation['timestamp'].min()).sum(),
#             (validation['timestamp'] > test['timestamp'].min()).sum()
#         )
#         assert train_late_count == 0, f"Found {train_late_count} records in train that are later than validation start"
#         assert val_late_count == 0, f"Found {val_late_count} records in validation that are later than test start"
#
#
#
#     check_no_cross_timeline_leakage(train, validation, test)
#
#     check_timestamps_sorted(train, "train")
#     check_timestamps_sorted(validation, "validation")
#     check_timestamps_sorted(test, "test")



def test_user_timeline_consistency(real_ratings_ddf):
    """Проверка утечек времени для пользователей"""

    train, validation, test = train_validation_test_split_ddf_on_users(
        real_ratings_ddf, test_ratio=0.1, validation_ratio=0.1
    )

    # 1. Вычисляем границы для каждого пользователя ОДИН РАЗ
    # Используем groupby вместо циклов
    user_stats = dask.compute(
        # Для train: максимальное время
        train.groupby('userId')['timestamp'].max().rename('train_max'),
        # Для validation: минимальное и максимальное время
        validation.groupby('userId')['timestamp'].min().rename('val_min'),
        validation.groupby('userId')['timestamp'].max().rename('val_max'),
        # Для test: минимальное время
        test.groupby('userId')['timestamp'].min().rename('test_min'),
        # Количество записей для проверки пустых выборок
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
            f"{violations_train_val.index.tolist()[:10]}"  # Показываем только первых 10
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
        # Для каждого пользователя проверяем что max >= min (быстрая проверка)
        user_min_max = df.groupby('userId')['timestamp'].agg(['min', 'max']).compute()

        # Если min > max - явная ошибка
        if (user_min_max['min'] > user_min_max['max']).any():
            # Детальная проверка только для проблемных пользователей
            problem_users = user_min_max[user_min_max['min'] > user_min_max['max']].index

            for user_id in problem_users[:10]:  # Проверяем только первых 10
                user_times = df[df['userId'] == user_id]['timestamp'].compute().values
                if len(user_times) > 1:
                    is_sorted = np.all(user_times[:-1] <= user_times[1:])
                    assert is_sorted, f"Timestamps not sorted for user {user_id} in {name}"

    check_sorted_per_user_vectorized(train, "train")
    check_sorted_per_user_vectorized(validation, "validation")
    check_sorted_per_user_vectorized(test, "test")
@pytest.mark.quality
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
    """Пара (userId, movieId) должна быть уникальной в ratings"""

    grouped = real_ratings_ddf.groupby(['userId', 'movieId']).size()
    has_duplicates = (grouped > 1).any().compute()
    assert not has_duplicates, "Найдены дубликаты user-movie пар в ratings"

def test_movieId_in_movies_exists_in_ratings(real_ratings_ddf, real_movies_ddf):
    """Все movieId из ratings должны быть в movies."""
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

    # user_id_is_integer = ratings['userId'].map_partitions(
    #     lambda x: (x == x.astype('int64')).all()
    # ).compute()
    # movie_id_is_integer = ratings['movieId'].map_partitions(
    #     lambda x: (x == x.astype('int64')).all()
    # ).compute()
    # assert user_id_is_integer, "userId не целые"
    # assert movie_id_is_integer, "movieId не целые"
def test_rating_range(real_ratings_ddf):
    """ Рейтинг должен быть от 0.5 до 5 """
    invalid = real_ratings_ddf['rating'].map_partitions(
        lambda part: (part < 0.5) | (part > 5.0)
    ).any().compute()

    assert not invalid, "Некорректные рейтинги обнаружены: значение вне диапазона [0.5, 5.0]"

# def test_user_item_coverage():
#     """Проверяем минимальное число оценок на пользователя и фильм."""
#     min_ratings_per_user = 5
#     min_ratings_per_movie = 3
#
#     user_counts = ratings['userId'].value_counts().compute()
#     movie_counts = ratings['movieId'].value_counts().compute()
#
#     low_users = user_counts[user_counts < min_ratings_per_user]
#     low_movies = movie_counts[movie_counts < min_ratings_per_movie]
#
#     assert len(low_users) / len(user_counts) < 0.1, \
#         f"Слишком много пользователей с <{min_ratings_per_user} оценками: {len(low_users)/len(user_counts):.2%}"
#     assert len(low_movies) / len(movie_counts) < 0.1, \
#         f"Слишком много фильмов с <{min_ratings_per_movie} оценками: {len(low_movies)/len(movie_counts):.2%}"