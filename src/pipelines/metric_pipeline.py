
from typing import List, Callable, Dict
import numpy as np
import pandas as pd

from src.utils.metrics import RecommendationMetrics


class MetricPipeline:
    def __init__(self, k_list: List[int], metrics: List[str] = None):
        self.k_list = sorted(k_list)
        self.metrics = metrics

        self.available_metrics = {
            "Precision": RecommendationMetrics.precision_at_k,
            "Recall": RecommendationMetrics.recall_at_k,
            "MAP": RecommendationMetrics.map_at_k,
            "NDCG": RecommendationMetrics.ndcg_at_k,

        }

        # Проверяем, что все запрошенные метрики реализованы
        for metric in self.metrics:
            if metric not in self.available_metrics:
                raise ValueError(f"Метрика '{metric}' не реализована в RecommendationMetrics")

    def _calculate_metric_for_user(
            self,
            metric_func: Callable,
            recommended: List[int],
            relevant: List[int],
            k: int
    ) -> float:
        try:
            return metric_func(recommended, relevant, k)
        except (ZeroDivisionError, IndexError, ValueError):
            return 0.0

    def _calculate_metric_for_group_of_users(
        self,
        metric_name: str,
        recommendations: List[List[int]],
        relevant: List[List[int]],
        k: int
    ) -> float:
        metric_func = self.available_metrics[metric_name]

        if metric_name == "MAP":
            return metric_func(recommendations, relevant, k)
        else:
            user_scores = [
                self._calculate_metric_for_user(metric_func, rec, rel, k)
                for rec, rel in zip(recommendations, relevant)
            ]

            return np.mean(user_scores) if user_scores else 0.0

    def calculate_metrics_for_model(
            self,
            model_name: str,
            recommendations: List[List[int]],
            relevant: List[List[int]],
    ) -> Dict[str, dict]:
        results = {}
        for metric_name in self.metrics:
            for k in self.k_list:
                score = self._calculate_metric_for_group_of_users(metric_name, recommendations, relevant, k)
                results[f"{metric_name}@{k}"] = score
        return {model_name: results}

    def run(
        self,
        model_recommendations: Dict[str, List[List[int]]],
        relevant_items: List[List[int]],

    ) -> pd.DataFrame:

        all_result = {}
        for model_name, recommendations in model_recommendations.items():
            model_results = self.calculate_metrics_for_model(model_name, recommendations, relevant_items)
            all_result.update(model_results)
        result_df = pd.DataFrame(all_result).T
        result_df.index.name = "Model"
        return result_df