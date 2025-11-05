# import dask.dataframe as dd
# import pandas as pd
# import pytest
#
#
# try:
#     ratings = dd.read_csv("data_csv/input/rating.csv")
#     movies = dd.read_csv("data_csv/input/movie.csv")
#
#     ratings['timestamp'] = dd.to_datetime(ratings['timestamp'])
#
# except Exception as ex:
#     raise FileNotFoundError(f"Не удалось загрузить данные: {ex}")
# try:
#     ratings.astype({
#         'userId': 'int64',
#         'movieId': 'int64',
#         'rating': 'float64',
#         'timestamp': 'datetime64[ns]'
#     })
#
#
# except KeyError as ex:
#     raise KeyError(f"Ошибка наличия столбца: {ex} ")
# except TypeError as ex:
#     raise TypeError(f"Неподдерживаемый тип: {ex}")
#
#
# # def test_data_structure():
# #     """ Проверка наличия столбцов """
# #
# #     assert 'userId' in ratings.columns, "Нет столбца userId в ratings"
# #     assert 'movieId' in ratings.columns, "Нет столбца movieId в ratings"
# #     assert 'rating' in ratings.columns, "Нет столбца rating в ratings"
# #     assert 'timestamp' in ratings.columns, "Нет столбца timestamp в ratings"
# #
# #     dtypes = ratings.dtypes.compute()
# #     assert dtypes['userId'] in ['int64', 'int32'], "userId должен быть целым"
# #     assert dtypes['movieId'] in ['int64', 'int32'], "movieId должен быть целым"
# #     assert dtypes['rating'] in ['float64', 'float32'], "rating должен быть целым"
# #     assert dtypes['timestamp'] in ['datetime64[ns]'], "timestamp должен быть целым"
#
# def test_no_missing_values():
#     null_count_ratings = ratings.isnull().sum().sum().compute()
#     assert null_count_ratings == 0, \
#         f"Найдены пропущенные значения в ratings: {null_count_ratings}"
#
#     null_count_movies = movies.isnull().sum().sum().compute()
#     assert null_count_movies == 0, \
#         f"Найдены пропущенные значения в movies: {null_count_movies}"
#
#
# def test_unique_user_movie_pairs():
#     """Пара (userId, movieId) должна быть уникальной в ratings"""
#
#     grouped = ratings.groupby(['userId', 'movieId']).size()
#     has_duplicates = (grouped > 1).any().compute()
#     assert not has_duplicates, "Найдены дубликаты user-movie пар в ratings"
#
# def test_movieId_in_movies_exists_in_ratings():
#     """Все movieId из ratings должны быть в movies."""
#     movie_ids_ratings = ratings['movieId'].unique()
#     movie_ids_movies = movies['movieId'].unique()
#
#
#     anti_join = movie_ids_ratings.map_partitions(
#         lambda x: x[~x.isin(movie_ids_movies.compute())]
#     )
#     missing = anti_join.compute()
#
#     assert len(missing) == 0, f"В ratings есть movieId, отсутствующие в movies: {len(missing)} шт."
#
# def test_consistency():
#     """userId и movieId должны быть положительными"""
#     min_user_id = ratings['userId'].min().compute()
#     min_movie_id = ratings['movieId'].min().compute()
#     assert min_user_id >= 0, "userId должны быть >= 0"
#     assert min_movie_id >= 0, "movieId должны быть >= 0"
#
#     # user_id_is_integer = ratings['userId'].map_partitions(
#     #     lambda x: (x == x.astype('int64')).all()
#     # ).compute()
#     # movie_id_is_integer = ratings['movieId'].map_partitions(
#     #     lambda x: (x == x.astype('int64')).all()
#     # ).compute()
#     # assert user_id_is_integer, "userId не целые"
#     # assert movie_id_is_integer, "movieId не целые"
# def test_rating_range():
#     """ Рейтинг должен быть от 0.5 до 5 """
#     invalid = ratings['rating'].map_partitions(
#         lambda part: (part < 0.5) | (part > 5.0)
#     ).any().compute()
#
#     assert not invalid, "Некорректные рейтинги обнаружены: значение вне диапазона [0.5, 5.0]"
#
# # def test_user_item_coverage():
# #     """Проверяем минимальное число оценок на пользователя и фильм."""
# #     min_ratings_per_user = 5
# #     min_ratings_per_movie = 3
# #
# #     user_counts = ratings['userId'].value_counts().compute()
# #     movie_counts = ratings['movieId'].value_counts().compute()
# #
# #     low_users = user_counts[user_counts < min_ratings_per_user]
# #     low_movies = movie_counts[movie_counts < min_ratings_per_movie]
# #
# #     assert len(low_users) / len(user_counts) < 0.1, \
# #         f"Слишком много пользователей с <{min_ratings_per_user} оценками: {len(low_users)/len(user_counts):.2%}"
# #     assert len(low_movies) / len(movie_counts) < 0.1, \
# #         f"Слишком много фильмов с <{min_ratings_per_movie} оценками: {len(low_movies)/len(movie_counts):.2%}"