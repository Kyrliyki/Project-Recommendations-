import numpy as np


class Metrics:
    @staticmethod
    def rmse(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
            k: int,
    ) -> float:
        """
        подсчет RMSE
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def precision(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
            k: int,
    ) -> float:
        """
        подсчет Precision
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def recall(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
            k: int,
    ) -> float:
        """
        подсчет Recall
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def map(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
            k: int,
    ) -> float:
        """
        подсчет MAP
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        pass

    @staticmethod
    def ndcg(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
            k: int,
    ) -> float:
        """
        подсчет NDCG
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        pass