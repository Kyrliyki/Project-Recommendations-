import pytest
import numpy as np
from src.utils.metrics import RecommendationMetrics
from math import log2



precision_at_k = RecommendationMetrics.precision_at_k
recall_at_k = RecommendationMetrics.recall_at_k
ap_at_k = RecommendationMetrics.ap_at_k
map_at_k = RecommendationMetrics.map_at_k
dcg_at_k = RecommendationMetrics.dcg_at_k
idcg_at_k = RecommendationMetrics.idcg_at_k
ndcg_at_k = RecommendationMetrics.ndcg_at_k

# @pytest.mark.test_metrics()
# @pytest.mark.parametrize(
#     "metric_func, relevant, recommended, k, expected",
#     [
#
#         # Все recommend релевантны
#
#         (precision_at_k,[1, 2, 3, 4, 5], [1, 2, 3, 4, 5], 5, 1),
#         (recall_at_k,[1, 2, 3, 4, 5], [1, 2, 3, 4, 5], 5, 1),
#         (map_at_k,[[1, 2, 3, 4, 5]], [[1, 2, 3, 4, 5]], 5, 1),
#         (ndcg_at_k,[1, 2, 3, 4, 5], [1, 2, 3, 4, 5], 5, 1),
#
#         # нет релевантных в топ k
#         (precision_at_k,[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], 5, 0),
#         (recall_at_k,[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], 5, 0),
#         (map_at_k,[[1, 2, 3, 4, 5]], [[6, 7, 8, 9, 10]], 5, 0),
#         (ndcg_at_k,[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], 5, 0),
#
#         # в recommend до k часть релевантных
#         (precision_at_k,[1, 3, 5, 7, 9], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5, 0.6),
#         (recall_at_k,[1, 3, 5, 7, 9], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5, 0.6),
#         (map_at_k,[[1, 3, 5, 7, 9]], [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], 5, (1/1 + 2/3 + 3/5) / 3),
#         (ndcg_at_k,[1, 3, 5, 7, 9], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5,
#          ((3/2 + 1 / math.log2(6)) / (3/2 + 1 / math.log2(3) + 1 / math.log2(5) + 1 / math.log2(6)))),
#
#
#         # K больше recommend и relevant
#         (precision_at_k,[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], 10, 0),
#         (recall_at_k,[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], 10, 0),
#         (map_at_k,[[1, 2, 3, 4, 5]], [[6, 7, 8, 9, 10]], 10, 0),
#         (ndcg_at_k,[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], 10, 0),
#
#
#         # пустые recommend
#         (precision_at_k,[1, 2, 3, 4, 5], [], 5, 0),
#         (recall_at_k,[1, 2, 3, 4, 5], [], 5, 0),
#         (map_at_k,[[1, 2, 3, 4, 5]], [[]], 5, 0),
#         (ndcg_at_k,[1, 2, 3, 4, 5], [], 5, 0),
#
#         # пустые relevant
#         (precision_at_k,[], [6, 7, 8, 9, 10], 5, 0),
#         (recall_at_k,[], [6, 7, 8, 9, 10], 5, 0),
#         (map_at_k,[[]], [[6, 7, 8, 9, 10]], 5, 0),
#         (ndcg_at_k,[], [6, 7, 8, 9, 10], 5, 0),
#     ]
# )
# def test_metrics(metric_func, relevant, recommended, k, expected):
#     result = metric_func(recommended, relevant, k)
#     assert np.isclose(result, expected), (
#         f"{metric_func.__name__}({relevant}, {recommended}, k={k}) = {result}, "
#         f"expected {expected}"
#     )

@pytest.mark.test_metrics()
@pytest.mark.parametrize("recommended, relevant, k , expected", [
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

# @pytest.mark.test_metrics()
# def test_precision_at_k_wrong_type():
#     with pytest.raises(TypeError):
#         precision_at_k("not a list", [1, 2, 3], 1)
#     with pytest.raises(TypeError):
#         precision_at_k([1, 2, 3], "not a list", 1)
#     with pytest.raises(TypeError):
#         precision_at_k([1, 2, 3], [1, 2, 3], 1.1)
#     with pytest.raises(TypeError):
#         precision_at_k([1, 2, 3], [1, 2, 3], "not a int")
# @pytest.mark.test_metrics()
# def test_precision_at_k_wrong_value():
#     with pytest.raises(ValueError):
#         precision_at_k([-1, 2, 3], [1, 2, 3], 1)
#     with pytest.raises(ValueError):
#         precision_at_k([1, 2, 3], [-1, 2, 3], 1)
#     with pytest.raises(ValueError):
#         precision_at_k([1, 2, 3], [1, 2, 3], -1)


@pytest.mark.test_metrics()
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

@pytest.mark.test_metrics()
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
@pytest.mark.test_metrics()
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


@pytest.mark.test_metrics()
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
