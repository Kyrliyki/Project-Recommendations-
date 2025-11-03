
import math
import numpy as np
from typing import List

class RecommendationMetrics:

    @staticmethod
    def precision_at_k(recommended: List[int], relevant: List[int], k: int) -> float:

        if k == 0 or not recommended:
            return 0.0
        top_k = recommended[:k]
        intesection = set(top_k).intersection(relevant)
        return len(intesection) / k

    @staticmethoda
    def recall_at_k(recommended: List[int], relevant: List[int], k: int) -> float:
        if not relevant:
            return 0.0
        top_k = recommended[:k]
        intesection = set(top_k).intersection(relevant)
        return len(intesection) / len(relevant)

    @staticmethod
    def ap_at_k(recommended: List[int], relevant: List[int], k: int) -> float:
        if k < 1 or not relevant:
            return 0.0

        recommended = list(dict.fromkeys(recommended))
        relevant_set = set(relevant)
        recommended_at_k = recommended[:k]
        score = 0.0
        num_hits = 0

        for i, item in enumerate(recommended_at_k):
            if item in relevant_set:
                num_hits += 1
                precision_at_i = num_hits / (i + 1)
                score += precision_at_i

        if num_hits == 0:
            return 0.0
        return score / num_hits

    @staticmethod
    def map_at_k(all_recommended: List[List[int]], all_relevant: List[List[int]], k: int) -> float:
        if k < 1:
            return 0.0
        ap_scores = []
        for relevant, recommended in zip(all_relevant, all_recommended):
            ap = RecommendationMetrics.ap_at_k(recommended, relevant, k)
            ap_scores.append(ap)
        return np.mean(ap_scores)

    @staticmethod
    def dcg_at_k(recommended: List[int], relevant: List[int], k: int):
        if k < 1 or not recommended or not relevant:
            return 0.0
        recommended = list(dict.fromkeys(recommended))
        relevant_set = set(relevant)
        relevances = [1 if item in relevant_set else 0 for item in recommended[:k]]

        dcg = 0.0

        for i, rel in enumerate(relevances, start=1):
            dcg += rel / math.log2(i + 1)

        return dcg

    @staticmethod
    def idcg_at_k(relevant: List[int], k: int):
        if k < 1 or not relevant:
            return 0.0
        relevant_set = set(relevant)

        max_possible = min(len(relevant_set), k)

        ideal_relevances = [1] * max_possible + [0] * (k - max_possible)
        idcg = 0.0
        for i, rel in enumerate(ideal_relevances, start=1):
            idcg += rel / math.log2(i + 1)

        return idcg

    @staticmethod
    def ndcg_at_k(recommended: List[int], relevant: List[int], k: int):
        if k < 1 or not recommended or not relevant:
            return 0.0
        dcg = RecommendationMetrics.dcg_at_k(recommended, relevant, k)
        idcg = RecommendationMetrics.idcg_at_k(relevant, k)

        if idcg == 0:
            return 0.0
        return dcg / idcg
