from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    List,
)

import dask.dataframe as dd


class MLModelBase(ABC):
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def fit(
            self,
            data: dd.DataFrame
    ) -> None:
        """
        обучение модели
            data - данные для обучения (train_set)
        """
        pass

    @abstractmethod
    def predict(
            self,
            user_id: int,
            movie_id: int,
    ) -> Any:
        """
        предсказание модели
            user_id: int - id пользователя
            movie_id: int - id фильма
        returning
            предсказанная оценка пользователя фильму
        """
        pass

    @abstractmethod
    def getting_recommended_movies(
            self,
            user_id: int,
            movies_list: List[int] | None,
            top_k: int,
    ) -> List[int]:
        """
        получение рекомендаций для пользователя
            user_id - id пользователя для персональных рекомендаций
            movie_list - список фильмов для выставления оценок
            expected_number_of_recommendations - ожидаемое количество рекомендованных фильмов
        returning
            массив id фильмов, отсортированный по убыванию рейтинга
        """
        pass
