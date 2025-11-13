import numpy as np

from config import settings


def binarize_with_threshold(
        data: np.ndarray,
        threshold: float = settings.metrics.threshold_for_binarize,
) -> np.ndarray:
    """
    бинаризация данных относительно порогового элемента
        data: np.ndarray - данные для преобразования
        threshold: float = settings.metrics.threshold_for_binarize - пороговое значение,
    returning
        бинаризированные данные
    """
    return np.where(np.array(data) > threshold, 1, 0)
