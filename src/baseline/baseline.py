from src.utils.config import settings
import pandas as pd
from typing import List, Optional


class Baseline:

    
    @staticmethod
    def _get_seen_movie_ids(rating: pd.DataFrame, user_id: int) -> set:
        return set(
            rating.loc[rating["userId"] == user_id, "movieId"].unique()
        )

    @staticmethod
    def bayesian_mean_baseline(
        movie: pd.DataFrame,
        rating: pd.DataFrame,
        user_id: int,
        n_recommendations: int = 10,
        m: int = 25,  # "минимальное" число голосов
    ) -> List[int]:
        """
        Рекомендует фильмы по байесовскому среднему:
        score = (v/(v+m))*R + (m/(v+m))*C
        R – средняя оценка фильма, v – число оценок,
        C – глобальная средняя по датасету, m - минимальное число голосов
        """

        seen_ids = Baseline._get_seen_movie_ids(rating, user_id)

        # глобальная средняя оценка
        C = rating["rating"].mean()

        stats = (
            rating.groupby("movieId")["rating"]
            .agg(mean_rating="mean", n_ratings="count")
            .reset_index()
        )

        # только фильмы, которые есть в таблице movie
        stats = stats.merge(movie[["movieId"]], on="movieId", how="inner")

        # байесовский скор
        v = stats["n_ratings"]
        R = stats["mean_rating"]
        stats["score"] = (v / (v + m)) * R + (m / (v + m)) * C

        # исключаем просмотренные
        stats = stats[~stats["movieId"].isin(seen_ids)]

        stats = stats.sort_values(["score", "n_ratings"],
                                  ascending=[False, False]).head(
            n_recommendations
        )

        return stats["movieId"].tolist()

    @staticmethod
    def recent_popularity_baseline(
        movie: pd.DataFrame,
        rating: pd.DataFrame,
        user_id: int,
        n_recommendations: int = 10,
        window_days: int = 180,  # смотрим популярность за последние 6 месяцев
    ) -> List[int]:
        """
        Фильмы, которые получили больше всего оценок за последние window_days.
        """

        if "timestamp" not in rating.columns:
            raise ValueError("В ratings нет столбца 'timestamp'")

        seen_ids = Baseline._get_seen_movie_ids(rating, user_id)

        ratings_ts = rating.copy()
        ratings_ts["timestamp"] = pd.to_datetime(
            ratings_ts["timestamp"], errors="coerce"
        )
        max_ts = ratings_ts["timestamp"].max()
        if pd.isna(max_ts):
            return []

        cutoff = max_ts - pd.Timedelta(days=window_days)
        recent = ratings_ts[ratings_ts["timestamp"] >= cutoff]

        if recent.empty:
            return []

        recent_pop = (
            recent.groupby("movieId")["userId"]
            .count()
            .rename("n_ratings_recent")
            .reset_index()
        )

        recent_pop = recent_pop.merge(
            movie[["movieId"]], on="movieId", how="inner"
        )
        recent_pop = recent_pop[~recent_pop["movieId"].isin(seen_ids)]

        recent_pop = recent_pop.sort_values(
            "n_ratings_recent", ascending=False
        ).head(n_recommendations)

        return recent_pop["movieId"].tolist()
    
    @staticmethod
    def random_baseline(
        movie: pd.DataFrame,
        rating: pd.DataFrame,
        user_id: int,
        n_recommendations: int = 10,
        random_state: Optional[int] = None,
    ) -> List[int]:
        """
        Случайные фильмы из ещё не оценённых пользователем.
        """
        if random_state is None and hasattr(settings, "RANDOM_STATE"):
            random_state = settings.RANDOM_STATE

        seen_ids = Baseline._get_seen_movie_ids(rating, user_id)
        candidates = movie[~movie["movieId"].isin(seen_ids)]

        if candidates.empty:
            return []

        n = min(n_recommendations, len(candidates))
        recs = candidates.sample(n=n, random_state=random_state)

        return recs["movieId"].tolist()

    @staticmethod
    def popularity_baseline(
        movie: pd.DataFrame,
        rating: pd.DataFrame,
        user_id: int,
        n_recommendations: int = 10,
    ) -> List[int]:
        """
        Топ фильмов по числу оценок (популярность),
        которые пользователь ещё не смотрел.
        """
        seen_ids = Baseline._get_seen_movie_ids(rating, user_id)

        popularity = (
            rating.groupby("movieId")["userId"]
            .count()
            .rename("n_ratings")
            .reset_index()
        )

        # только фильмы, которые есть в таблице movie
        popularity = popularity.merge(
            movie[["movieId"]], on="movieId", how="inner"
        )

        # исключаем уже просмотренные
        popularity = popularity[~popularity["movieId"].isin(seen_ids)]

        popularity = (
            popularity.sort_values("n_ratings", ascending=False)
            .head(n_recommendations)
        )

        return popularity["movieId"].tolist()

    @staticmethod
    def mean_rating_baseline(
        movie: pd.DataFrame,
        rating: pd.DataFrame,
        user_id: int,
        n_recommendations: int = 10,
        min_n_ratings: int = 25,
    ) -> List[int]:
        """
        Топ фильмов по средней оценке (при min_n_ratings голосах),
        которые пользователь ещё не смотрел.
        """
        seen_ids = Baseline._get_seen_movie_ids(rating, user_id)

        stats = (
            rating.groupby("movieId")["rating"]
            .agg(mean_rating="mean", n_ratings="count")
            .reset_index()
        )

        stats = stats[stats["n_ratings"] >= min_n_ratings]

        stats = stats.merge(movie[["movieId"]], on="movieId", how="inner")
        stats = stats[~stats["movieId"].isin(seen_ids)]

        stats = (
            stats.sort_values(
                ["mean_rating", "n_ratings"],
                ascending=[False, False],
            )
            .head(n_recommendations)
        )

        return stats["movieId"].tolist()    


if __name__ == "__main__":
    movie_csv = pd.read_csv(settings.data.path_to_movie_csv)
    rating_csv = pd.read_csv(settings.data.path_to_rating_csv)

    baseline = Baseline()
    print("\n Байесовское среднее")
    print(baseline.bayesian_mean_baseline(movie_csv,rating_csv, 1))
    print("\n Обычное среднее")
    print(baseline.mean_rating_baseline(movie_csv, rating_csv, 1))
    print("\n По популярности недавнего")
    print(baseline.recent_popularity_baseline(movie_csv, rating_csv, 1))
    print("\n Случайное ")
    print(baseline.random_baseline(movie_csv, rating_csv, 1))


