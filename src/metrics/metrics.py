import numpy as np
from sklearn import metrics as sklearn_metrics

from metrics.errror_handlers import check_length_error
from metrics.utils import binarize_with_threshold


class Metrics:
    @staticmethod
    def rmse(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
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
    def confusion_matrix(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
    ) -> np.ndarray:
        """
        подсчет Confusion Matrix
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            подсчитанная матрица ошибок: np.ndarray
        """
        check_length_error(len(y_true), len(y_predicted))

        y_true_binary = binarize_with_threshold(y_true)
        y_predicted_binary = binarize_with_threshold(y_predicted)

        confusion_matrix = sklearn_metrics.confusion_matrix(
            y_true=y_true_binary,
            y_pred=y_predicted_binary,
        )

        return confusion_matrix

    @staticmethod
    def precision_score(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
    ) -> float:
        """
        подсчет Precision
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        check_length_error(len(y_true), len(y_predicted))

        y_true_binary = binarize_with_threshold(y_true)
        y_predicted_binary = binarize_with_threshold(y_predicted)

        precision_score = sklearn_metrics.precision_score(
            y_true=y_true_binary,
            y_pred=y_predicted_binary,
        )

        return precision_score

    @staticmethod
    def recall_score(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
    ) -> float:
        """
        подсчет Recall
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        check_length_error(len(y_true), len(y_predicted))

        y_true_binary = binarize_with_threshold(y_true)
        y_predicted_binary = binarize_with_threshold(y_predicted)

        recall_score = sklearn_metrics.recall_score(
            y_true=y_true_binary,
            y_pred=y_predicted_binary,
        )

        return recall_score

    @staticmethod
    def map_score(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
    ) -> float:
        """
        подсчет AP (для одного класса)
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        check_length_error(len(y_true), len(y_predicted))

        thresholds = np.arange(start=2, stop=4, step=0.2)
        ap_scores = np.array([])
        for threshold in thresholds:
            y_true_binary = binarize_with_threshold(
                data=y_true,
                threshold=threshold,
            )
            ap_scores = np.append(
                ap_scores,
                sklearn_metrics.average_precision_score(
                    y_true=y_true_binary,
                    y_pred=y_predicted,
                )
            )


        map_score = np.sum(ap_scores)
        return map_score

    @staticmethod
    def ndcg_score(
            y_true: np.ndarray,
            y_predicted: np.ndarray,
    ) -> float:
        """
        подсчет NDCG
            y_true: np.ndarray - правильные оценки
            y_predicted: np.ndarray - предсказанные оценки
        returning
            значение подсчитанной метрики: float
        """
        check_length_error(len(y_true), len(y_predicted))
        max_rating = max(
            max(y_true),
            max(y_predicted),
        )
        y_true_normalized = y_true / max_rating
        y_predicted_normalized = y_predicted / max_rating

        ndcg_score = sklearn_metrics.ndcg_score(
            y_true= y_true_normalized,
            y_score=y_predicted_normalized,
        )
        return ndcg_score