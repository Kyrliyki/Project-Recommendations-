from abc import (
    ABC,
    abstractmethod,
)
import dask.dataframe as dd
from pydantic import BaseModel


class MetricsScheme(BaseModel):
    RMSE: float
    Precision: float
    Recall: float
    MAP: float
    NDCG: float


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
            user_id - id пользователя для персональных рекомендаций
            expected_number_of_recommendations - ожидаемое количество рекомендованных фильмов
        returning
            id рекомендованных фильмов
        """
        pass

    @abstractmethod
    def calculating_metrics(
            self,
            test_data: dd.DataFrame
    ) -> MetricsScheme:
        """
        подсчет метрик модели
            test_data - данные для тестирования (test_set)
            expected_number_of_recommendations - ожидаемое количество рекомендованных фильмов
        returning
            MetricsScheme(
                "RMSE": float_value,
                "Precision": float_value,
                "Recall": float_value,
                "MAP": float_value,
                "NDCG": float_value,
            )
        """
        pass