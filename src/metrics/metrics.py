import numpy as np

from metrics.errror_handlers import check_length_error
from metrics.utils import binarize_with_pivot_value


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
        diff = y_true - y_predicted
        differences_squared = diff ** 2
        mean_diff = differences_squared.mean()
        rmse_value = np.sqrt(mean_diff)
        return rmse_value

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
        check_length_error(len(y_true), len(y_predicted))

        y_true_binary = binarize_with_pivot_value(y_true)
        y_predicted_binary = binarize_with_pivot_value(y_predicted)

        tp = np.sum((y_true_binary == 1) & (y_predicted_binary == 1))
        fp = np.sum((y_true_binary == 0) & (y_predicted_binary == 1))

        if tp + fp == 0:
            return 0

        precision = tp / (tp + fp)
        return precision

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
        check_length_error(len(y_true), len(y_predicted))

        y_true_binary = binarize_with_pivot_value(y_true)
        y_predicted_binary = binarize_with_pivot_value(y_predicted)

        tp = np.sum((y_true_binary == 1) & (y_predicted_binary == 1))
        fn = np.sum((y_true_binary == 0) & (y_predicted_binary == 0))

        if tp + fn == 0:
            return 0

        recall = tp / (tp + fn)
        return recall

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