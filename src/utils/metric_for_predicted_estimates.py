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


def precision_score_macro(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
) -> float:
    """
    подсчет Precision c macro усреднением
        y_true: np.ndarray - правильные оценки
        y_predicted: np.ndarray - предсказанные оценки
        max_mae: float - максимальная погрешность
    returning
        значение подсчитанной метрики: float
    """
    y_length = len(y_true)
    rating_classes = set(y_true)
    rating_classes_scores = []
    for rating_class in rating_classes:
        tp = 0
        fn = 0
        for index in range(y_length):
            true_rating = y_true[index]
            current_mae = abs(true_rating - y_predicted[index])
            if true_rating == rating_class:
                if current_mae <= max_mae:
                    tp += 1
                else:
                    fn += 1
        result_for_current_class = tp / (tp + fn)
        rating_classes_scores.append(result_for_current_class)
    result_precision = sum(rating_classes_scores) / len(rating_classes_scores)
    return result_precision


def precision_score_micro(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
) -> float:
    """
    подсчет Precision c micro усреднением
        y_true: np.ndarray - правильные оценки
        y_predicted: np.ndarray - предсказанные оценки
        max_mae: float - максимальная погрешность
    returning
        значение подсчитанной метрики: float
    """
    y_length = len(y_true)
    rating_classes = set(y_true)
    rating_classes_tp = []
    rating_classes_fn = []
    for rating_class in rating_classes:
        tp = 0
        fn = 0
        for index in range(y_length):
            true_rating = y_true[index]
            current_mae = abs(true_rating - y_predicted[index])
            if true_rating == rating_class:
                if current_mae <= max_mae:
                    tp += 1
                else:
                    fn += 1
        rating_classes_tp.append(tp)
        rating_classes_fn.append(fn)
    mean_tp = sum(rating_classes_tp) / len(rating_classes_tp)
    mean_fn = sum(rating_classes_fn) / len(rating_classes_fn)
    result_precision = mean_tp / (mean_tp + mean_fn)
    return result_precision


def precision_k(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
        average: str = "macro",
        k: int | None = None,
) -> float:
    """
    подсчет Precision по предсказанным оценкам
        y_true: np.ndarray - правильные оценки
        y_predicted: np.ndarray - предсказанные оценки
        max_mae: float - максимальная погрешность
        average: str - усреднение
        k: int - количество проверяемых предсказанных оценок,
    returning
        значение подсчитанной метрики: float
    """
    check_length_error(len(y_true), len(y_predicted))
    if k:
        y_true = y_true[:k]
        y_predicted = y_predicted[:k]

    result = 0.0
    if average == "macro":
        result = precision_score_macro(
            y_true=y_true,
            y_predicted=y_predicted,
            max_mae=max_mae,
        )
    elif average == "micro":
        result = precision_score_micro(
            y_true=y_true,
            y_predicted=y_predicted,
            max_mae=max_mae,
        )
    return result