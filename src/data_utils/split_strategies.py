from abc import ABC, abstractmethod
from typing import Tuple, Dict, Any
import dask.dataframe as dd
from src.data_utils.preparing_data import train_validation_test_split_ddf
from src.data_utils.preparing_data import train_validation_test_split_ddf_on_users


class SplitStrategy(ABC):
    @abstractmethod
    def split(self, data: dd.DataFrame) -> Tuple[dd.DataFrame, dd.DataFrame, dd.DataFrame]:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_params(self) -> Dict[str, Any]:
        pass


class TimeSplitStrategy(SplitStrategy):
    """Стратегия разделения по времени"""

    def __init__(
            self,
            test_ratio: float,
            validation_ratio: float
    ):
        self.test_ratio = test_ratio
        self.validation_ratio = validation_ratio

        self.split_func = train_validation_test_split_ddf

    def split(self, data: dd.DataFrame) -> Tuple[dd.DataFrame, dd.DataFrame, dd.DataFrame]:
        return self.split_func(
            data=data,
            validation_ratio=self.validation_ratio,
            test_ratio=self.test_ratio
        )

    def get_name(self) -> str:
        return "time_based"

    def get_params(self) -> Dict[str, Any]:
        return {
            "test_ratio": self.test_ratio,
            "validation_ratio": self.validation_ratio,
            "type": "time_based"
        }


class UserSplitStrategy(SplitStrategy):
    """Стратегия разделения по пользователям"""

    def __init__(
            self,
            test_ratio: float,
            validation_ratio: float
    ):
        self.test_ratio = test_ratio
        self.validation_ratio = validation_ratio
        self.split_func = train_validation_test_split_ddf_on_users

    def split(self, data: dd.DataFrame) -> Tuple[dd.DataFrame, dd.DataFrame, dd.DataFrame]:
        return self.split_func(
            data=data,
            validation_ratio=self.validation_ratio,
            test_ratio=self.test_ratio
        )

    def get_name(self) -> str:
        return "user_based"

    def get_params(self) -> Dict[str, Any]:
        return {
            "test_ratio": self.test_ratio,
            "validation_ratio": self.validation_ratio,
            "type": "user_based"
        }


SPLIT_STRATEGIES = {
    "time": TimeSplitStrategy,
    "user": UserSplitStrategy,
}


def get_split_strategy(name: str, **kwargs) -> SplitStrategy:
    if name not in SPLIT_STRATEGIES:
        raise ValueError(
            f"Unknown split strategy '{name}'. "
            f"Available: {list(SPLIT_STRATEGIES.keys())}"
        )

    return SPLIT_STRATEGIES[name](**kwargs)
