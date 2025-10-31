import numpy as np

from config import settings


def binarize_with_pivot_value(
        data: np.ndarray,
        pivot: float = settings.metrics.pivot_for_binarize,
) -> np.ndarray:
    """
    бинаризация данных относительно порогового элемента
        data: np.ndarray - данные для преобразования
        pivot: float = settings.metrics.pivot_for_binarize - пороговое значение,
    returning
        бинаризированные данные
    """
    vectorized_function = np.vectorize(
        lambda x:
        1 if x > pivot
        else 0
    )
    result = vectorized_function(data)
    return result
