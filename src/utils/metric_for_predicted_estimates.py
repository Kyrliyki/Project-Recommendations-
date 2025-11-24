from typing import List


def check_length_error(
        len_first: int,
        len_second: int,
) -> None:
    if len_first != len_second:
        raise ValueError("Массивы должны иметь одинаковую длину")


def accuracy_k(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
        k: int | None = None,
) -> float:
    """
    подсчет Accuracy по предсказанным оценкам
        y_true: np.ndarray - правильные оценки
        y_predicted: np.ndarray - предсказанные оценки
        max_mae: float - максимальная погрешность
        k: int - количество проверяемых предсказанных оценок,
    returning
        значение подсчитанной метрики: float
    """
    check_length_error(len(y_true), len(y_predicted))
    if k:
        y_true = y_true[:k]
        y_predicted = y_predicted[:k]

    y_length = len(y_true)
    relevant_ratings_count = 0
    for index in range(y_length):
        current_mae = abs(y_true[index] - y_predicted[index])
        if current_mae <= max_mae:
            relevant_ratings_count += 1

    result_accuracy = relevant_ratings_count / y_length
    return result_accuracy