from typing import (
    List,
    Dict,
    Callable,
)
import numpy as np
import pandas as pd

from src.utils.metric_for_predicted_estimates import (
    accuracy_k,
    precision_k,
    recall_k,
)


class MetricForPredictedEstimatesPipeline:
    def __init__(
            self,
            k_list: List[int|None],
            max_mae: List[float],
            metrics: List[str] | None = None,
    ):
        self.k_list = k_list
        self.max_mae = max_mae
        all_metrics = {
            "Accuracy" : accuracy_k,
            "Precision": precision_k,
            "Recall": recall_k,
        }

        if metrics is None:
            self.metrics = all_metrics
        else:
            for metric in metrics:
                if metric not in all_metrics:
                    raise ValueError(f"Метрика '{metric}' не реализована в RecommendationMetrics")
            self.metrics = {
                key: value for key, value in all_metrics.items() if key in metrics
            }

    @staticmethod
    def _calculate_metrics_for_users_with_average(
            y_true: List[List[float]],
            y_predicted: List[List[float]],
            metric_function: Callable,
            average: str,
            k: int,
            max_mae: float,
    ) -> float:
        user_scores = [
            metric_function(
                y_true=true,
                y_predicted=predicted,
                max_mae=max_mae,
                average=average,
                k=k,
            )
            for true, predicted in zip(y_true, y_predicted)
        ]
        result = np.mean(user_scores) if user_scores else 0.0
        return result

    @staticmethod
    def _calculate_metrics_for_users(
            y_true: List[List[float]],
            y_predicted: List[List[float]],
            metric_function: Callable,
            k: int,
            max_mae: float,
    ) -> float:
        user_scores = [
            metric_function(
                y_true=true,
                y_predicted=predicted,
                max_mae=max_mae,
                k=k,
            )
            for true, predicted in zip(y_true, y_predicted)
        ]
        result = np.mean(user_scores) if user_scores else 0.0
        return result

    def _calculate_metrics_for_model(
            self,
            model_name: str,
            y_true: List[List[float]],
            y_predicted: List[List[float]],
    ) -> List[Dict[str, dict]]:
        results = []
        for current_max_mae in self.max_mae:
            calculating_scores = {
                "Range": current_max_mae
            }
            for m_name, m_func in self.metrics.items():
                for k in self.k_list:
                    if m_name in ["Precision", "Recall"]:
                        for average in ["micro", "macro"]:
                            score = self._calculate_metrics_for_users_with_average(
                                y_true=y_true,
                                y_predicted=y_predicted,
                                metric_function=m_func,
                                average=average,
                                k=k,
                                max_mae=current_max_mae,
                            )
                            if k is None:
                                calculating_scores[f"{m_name}({average})"] = score
                            else:
                                calculating_scores[f"{m_name}@{k}({average})"] = score
                    else:
                        score = self._calculate_metrics_for_users(
                            y_true=y_true,
                            y_predicted=y_predicted,
                            metric_function=m_func,
                            k=k,
                            max_mae=current_max_mae,
                        )
                        if k is None:
                            calculating_scores[f"{m_name}"] = score
                        else:
                            calculating_scores[f"{m_name}@{k}"] = score
            results.append(calculating_scores)
        return results

    def run(
            self,
            model_name: str,
            y_true: List[List[float]],
            y_predicted: List[List[float]],
    ):
        model_results = self._calculate_metrics_for_model(
            model_name = model_name,
            y_true=y_true,
            y_predicted=y_predicted,
        )
        result_df = pd.DataFrame(model_results)
        return result_df