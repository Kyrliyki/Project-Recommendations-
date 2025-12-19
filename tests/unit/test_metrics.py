import pytest
import numpy as np

from src.utils.metric_for_predicted_estimates import check_length_error, accuracy_k, precision_score_macro, \
    precision_score_micro, precision_k, recall_score_macro, recall_k, recall_score_micro
from src.utils.metrics import RecommendationMetrics
from math import log2



precision_at_k = RecommendationMetrics.precision_at_k
recall_at_k = RecommendationMetrics.recall_at_k
ap_at_k = RecommendationMetrics.ap_at_k
map_at_k = RecommendationMetrics.map_at_k
dcg_at_k = RecommendationMetrics.dcg_at_k
idcg_at_k = RecommendationMetrics.idcg_at_k
ndcg_at_k = RecommendationMetrics.ndcg_at_k


@pytest.mark.test_metrics
@pytest.mark.parametrize("recommended, relevant, k , expected", [
    ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [1, 3, 5], 1, 1),
    # В топ k все релевантны
    ([1, 2, 3], [1, 2, 3], 3, 1),
    # В топ k нет релевантных
    ([1, 2, 3], [4, 5, 6], 3, 0),
    # В топ k часть релевантных
    ([1, 2, 3], [4, 1, 2], 3, 2/3),
    # k > длины recommended
    ([1, 2, 3], [1, 2], 5, 2/5),
    # Пустой recommended
    ([], [4, 5, 6], 3, 0),
    # Пустой relevant
    ([1, 2, 3], [], 3, 0),
    # Дубликаты в recommend
    ([1, 1, 2, 2, 3, 4], [1, 2], 5, 2/5),

])
def test_precision_at_k(recommended, relevant, k, expected):
    result = precision_at_k(recommended, relevant, k)
    assert np.isclose(result, expected), (f"Precision@{k} = {result}, expected {expected}")

@pytest.mark.test_metrics
@pytest.mark.parametrize("recommended, relevant, k , expected", [
    # В топ k все релевантны
    ([1, 2, 3], [1, 2, 3], 3, 1),
    # В топ k нет релевантных
    ([1, 2, 3], [4, 5, 6], 3, 0),
    # В топ k часть релевантных
    ([1, 2, 3], [4, 1, 2, 5], 3, 2/4),
    # k > длины relevant
    ([1, 2, 3], [1, 2], 5, 2/2),
    # Пустой recommended
    ([], [4, 5, 6], 3, 0),
    # Пустой relevant
    ([1, 2, 3], [], 3, 0),
    # Дубликаты в recommend
    ([1, 1, 2, 2, 3, 4], [1, 2], 5, 2/2),

])
def test_recall_at_k(recommended, relevant, k, expected):
    result = recall_at_k(recommended, relevant, k)
    assert np.isclose(result, expected), (f"Recall@{k} = {result}, expected {expected}")

@pytest.mark.test_metrics
@pytest.mark.parametrize("recommended, relevant, k , expected", [
    # В топ k все релевантны
    ([1, 2, 3], [1, 2, 3], 3, 1),
    # В топ k нет релевантных
    ([1, 2, 3], [4, 5, 6], 3, 0),
    # В топ k часть релевантных
    ([1, 2, 3], [4, 1, 2, 5], 3, (1/1 + 2/2) / 2),
    # k = 0
    ([1, 2, 3], [1, 2], 0, 0),
    # k > длины relevant
    ([1, 2, 3], [1, 2], 5, (1/1 + 2/2) / 2),
    # k > длины recommended
    ([1, 2, 3], [1, 2], 5, (1/1 + 2/2) / 2),
    # Пустой recommended
    ([], [4, 5, 6], 3, 0),
    # Пустой relevant
    ([1, 2, 3], [], 3, 0),
    # Дубликаты в recommend
    ([1, 1, 2, 2, 3, 4], [1, 2], 5, (1/1 + 2/2) / 2),
])
def test_ap_at_k(recommended, relevant, k, expected):
    result = ap_at_k(recommended, relevant, k)
    assert np.isclose(result, expected), (f"AP@{k} = {result}, expected {expected}")

@pytest.mark.test_metrics
@pytest.mark.parametrize("recommended, relevant, k , expected", [
    # В топ k все релевантны
    ([[1, 2, 3], [1, 2, 3]], [[1, 2, 3], [1, 2, 3]], 3, 1),
    # В топ k нет релевантных
    ([[1, 2, 3], [1, 2, 3]], [[4, 5, 6], [4, 5, 6]], 3, 0),
    # В топ k часть релевантных
    ([[1, 2, 3], [1, 2, 3]], [[4, 1, 2, 5], [4, 1, 2, 5]], 3, (1 / 1 + 2 / 2) / 2),
    # k = 0
    ([[1, 2, 3], [1, 2, 3]], [[1, 2], [1, 2]], 0, 0),
    # k > длины relevant
    ([[1, 2, 3], [1, 2, 3]], [[1, 2], [1, 2]], 5, (1 / 1 + 2 / 2) / 2),
    # k > длины recommended
    ([[1, 2, 3], [1, 2, 3]], [[1, 2], [1, 2]], 5, (1 / 1 + 2 / 2) / 2),
    # Пустой recommended
    ([[]], [[4, 5, 6], [4, 5, 6]], 3, 0),
    # Пустой relevant
    ([[1, 2, 3], [1, 2, 3]], [[]], 3, 0),
    # Дубликаты в recommend
    ([[1, 1, 2, 2, 3, 4], [1, 1, 2, 2, 3, 4]], [[1, 2], [1, 2]], 5, (1 / 1 + 2 / 2) / 2),
])
def test_map_at_k(recommended, relevant, k, expected):
    result = map_at_k(recommended, relevant, k)
    assert np.isclose(result, expected), (f"MAP@{k} = {result}, expected {expected}")


@pytest.mark.test_metrics
@pytest.mark.parametrize("recommended, relevant, k , expected", [
    #В топ k все релевантны
    ([1, 2, 3], [1, 2, 3], 3, 1 ),
    # В топ k нет релевантных
    ([1, 2, 3], [4, 5, 6], 3, 0),
    # В топ k часть релевантных
    ([1, 2, 3], [4, 1, 2, 5], 4,
     (1 / log2(2) + 1 / log2(3)) / (1 / log2(2) + 1 / log2(3) + 1 / log2(4) + 1 / log2(5))
     ),
    # k = 0
    ([1, 2, 3], [1, 2], 0, 0),
    # k > длины relevant
    ([1, 2, 3], [1, 2], 5,
     (1 / log2(2) + 1 / log2(3)) / (1 / log2(2) + 1 / log2(3))
     ),
    # k > длины recommended
    ([1, 2, 3], [1, 2], 5,
     (1 / log2(2) + 1 / log2(3)) / (1 / log2(2) + 1 / log2(3))
     ),
    # Пустой recommended
    ([], [4, 5, 6], 3, 0),
    # Пустой relevant
    ([1, 2, 3], [], 3, 0),
    # Дубликаты в recommend
    ([1, 1, 2, 2, 3, 4], [1, 2], 5,
     (1 / log2(2) + 1 / log2(3)) / (1 / log2(2) + 1 / log2(3))
     ),
])
def test_ndcg_at_k(recommended, relevant, k, expected):
    result = ndcg_at_k(recommended, relevant, k)
    assert np.isclose(result, expected), (f"NDCG@{k} = {result}, expected {expected}")


def test_check_length_error_ok():
    check_length_error(5, 5)


def test_check_length_error_raises():
    with pytest.raises(ValueError, match="Массивы должны иметь одинаковую длину"):
        check_length_error(5, 4)


@pytest.mark.parametrize("y_true, y_pred, max_mae, k, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, None, 0.75),
    ([5, 4.1, 3, 2], [5, 4, 1, 2], 0.1, 2, 1),
    ([], [], 1, None, 0)
])
def test_accuracy_k(y_true, y_pred, max_mae, k, expected):
    result = accuracy_k(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae,
        k=k
    )
    assert np.isclose(result, expected), (f"Accuracy@{k} = {result}, expected {expected}")

def test_accuracy_k_length_error():
    with pytest.raises(ValueError):
        accuracy_k([1, 2], [1], max_mae=1.0)


@pytest.mark.parametrize("y_true, y_pred, max_mae, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, (1/2 + 1 + 1 + 0) / 4 ),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, 1),
    ([], [], 1, 0),
])
def test_precision_macro(y_true, y_pred, max_mae, expected):

    result = precision_score_macro(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae
    )
    assert np.isclose(result, expected), (f"Macro_Precision = {result}, expected {expected}")


@pytest.mark.parametrize("y_true, y_pred, max_mae, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, (3/4) / (3/4 + 1/4)),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, 1),
    ([], [], 1, 0),
])
def test_precision_micro(y_true, y_pred, max_mae, expected):

    result = precision_score_micro(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae
    )
    assert np.isclose(result, expected), (f"Micro_Precision = {result}, expected {expected}")

@pytest.mark.parametrize("y_true, y_pred, max_mae, average, k, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "macro", None, (1/2 + 1 + 1 + 0) / 4),
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "macro", 2, (1/2 + 1) / 2),
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "micro", None, (3/4) / (3/4 + 1/4)),
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "micro", 2, 1 / (1 + 1/2)),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, "macro", None,  1),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, "micro", None,  1),
    ([], [], 0.1, "macro", None,  0),
    ([], [], 0.1, "micro", None,  0),
])
def test_precision_k(y_true, y_pred, max_mae, average, k, expected):
    result = precision_k(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae,
        average=average,
        k=k,
    )
    assert np.isclose(result, expected), (f"Precision_k({average}, k={k}) = {result}, expected {expected}")





@pytest.mark.parametrize("y_true, y_pred, max_mae, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, (1 + 1 + 1 + 0) / 4 ),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, 1),
    ([1, 1, 2, 2], [3, 3, 4, 4], 0.1, (0 + 0) / 2),
    ([], [], 1, 0)
])
def test_recall_macro(y_true, y_pred, max_mae, expected):

    result = recall_score_macro(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae
    )
    assert np.isclose(result, expected), (f"Macro_Recall = {result}, expected {expected}")


@pytest.mark.parametrize("y_true, y_pred, max_mae, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, (3/4) / (3/4 + 1/4)),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, 1),
    ([1, 1, 2, 2],[3, 3, 4, 4], 0.1, 0),
    ([], [], 1, 0)
])
def test_recall_micro(y_true, y_pred, max_mae, expected):
    result = recall_score_micro(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae
    )
    assert np.isclose(result, expected), (f"Micro_Recall = {result}, expected {expected}")

@pytest.mark.parametrize("y_true, y_pred, max_mae, average, k, expected", [
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "macro", None, (1 + 1 + 1 + 0) / 4),
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "macro", 2, (1 + 1) / 2),
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "micro", None, (3/4) / (3/4 + 1/4)),
    ([4, 3, 5, 2], [4, 3.5, 4.8, 1], 0.5, "micro", 2, 1),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, "macro", None, 1),
    ([5, 4, 1, 2], [5, 4, 1, 2], 0.1, "micro", None, 1),
    ([], [], 0.1, "macro", None, 0),
    ([], [], 0.1, "micro", None, 0),
])
def test_recall_k(y_true, y_pred, max_mae, average, k, expected):
    result = recall_k(
        y_true=y_true,
        y_predicted=y_pred,
        max_mae=max_mae,
        average=average,
        k=k,
    )
    assert np.isclose(result, expected), (f"Recall_k({average}, k={k}) = {result}, expected {expected}")