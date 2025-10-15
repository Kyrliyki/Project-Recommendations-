from abc import (
    ABC,
    abstractmethod,
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
    def getting_recommended_movies(
            self,
            user_id: int,
            expected_number_of_recommendations: int,
    ) -> list[str]:
        """
        получение рекомендаций для пользователя
            data - данные для тестирования (test_set)
            expected_number_of_recommendations - ожидаемое количество рекомендованных фильмов
        returning
            title рекомендованных фильмов
        """
        pass

    # TODO: метод для оценки качества модели (RMSE, MSE)
    # @abstractmethod
    # def calculating rmse(self, data: pd.DataFrame, ...) -> float:
    #     pass
    #
    # @abstractmethod
    # def calculating mse(self, data: pd.DataFrame, ...) -> float:
    #     pass