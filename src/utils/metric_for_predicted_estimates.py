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

    try:
        result_accuracy = relevant_ratings_count / y_length
    except ZeroDivisionError:
        result_accuracy = 0.0

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
        fp = 0
        for index in range(y_length):
            true_rating = y_true[index]
            current_mae = abs(true_rating - y_predicted[index])
            current_mae_for_current_class = abs(rating_class - y_predicted[index])
            if true_rating == rating_class:
                if current_mae <= max_mae:
                    tp += 1
            else:
                if current_mae_for_current_class <= max_mae:
                    fp += 1

        try:
            result_for_current_class = tp / (tp + fp)
        except ZeroDivisionError:
            result_for_current_class = 0.0
        rating_classes_scores.append(result_for_current_class)
    try:
        result_precision = sum(rating_classes_scores) / len(rating_classes_scores)
    except ZeroDivisionError:
        result_precision = 0.0

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
    rating_classes_fp = []
    for rating_class in rating_classes:
        tp = 0
        fp = 0
        for index in range(y_length):
            true_rating = y_true[index]
            current_mae = abs(true_rating - y_predicted[index])
            current_mae_for_current_class = abs(rating_class - y_predicted[index])
            if true_rating == rating_class:
                if current_mae <= max_mae:
                    tp += 1
            else:
                if current_mae_for_current_class <= max_mae:
                    fp += 1
        rating_classes_tp.append(tp)
        rating_classes_fp.append(fp)
    try:
        mean_tp = sum(rating_classes_tp) / len(rating_classes_tp)
    except ZeroDivisionError:
        mean_tp = 0.0
    try:
        mean_fn = sum(rating_classes_fp) / len(rating_classes_fp)
    except ZeroDivisionError:
        mean_fn = 0.0
    try:
        result_precision = mean_tp / (mean_tp + mean_fn)
    except ZeroDivisionError:
        result_precision = 0.0

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


def recall_score_macro(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
) -> float:
    """
    подсчет Recall c macro усреднением
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
        try:
            result_for_current_class = tp / (tp + fn)
        except ZeroDivisionError:
            result_for_current_class = 0.0
        rating_classes_scores.append(result_for_current_class)
    try:
        result_recall = sum(rating_classes_scores) / len(rating_classes_scores)
    except ZeroDivisionError:
        result_recall = 0.0

    return result_recall


def recall_score_micro(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
) -> float:
    """
    подсчет Recall c micro усреднением
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
    try:
        mean_tp = sum(rating_classes_tp) / len(rating_classes_tp)
    except ZeroDivisionError:
        mean_tp = 0.0
    try:
        mean_fn = sum(rating_classes_fn) / len(rating_classes_fn)
    except ZeroDivisionError:
        mean_fn = 0.0
    try:
        result_recall = mean_tp / (mean_tp + mean_fn)
    except ZeroDivisionError:
        result_recall = 0.0

    return result_recall


def recall_k(
        y_true: List[float],
        y_predicted: List[float],
        max_mae: float,
        average: str = "macro",
        k: int | None = None,
) -> float:
    """
    подсчет Recall по предсказанным оценкам
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
        result = recall_score_macro(
            y_true=y_true,
            y_predicted=y_predicted,
            max_mae=max_mae,
        )
    elif average == "micro":
        result = recall_score_micro(
            y_true=y_true,
            y_predicted=y_predicted,
            max_mae=max_mae,
        )
    return result