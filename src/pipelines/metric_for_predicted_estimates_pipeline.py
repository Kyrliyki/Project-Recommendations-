from typing import (
    List,
    Dict,
)
import numpy as np
import pandas as pd

from src.utils.metric_for_predicted_estimates import accuracy_k


class AccuracyScore:
    def __init__(
            self,
            k_list: List[int],
            max_mae: float,
    ):
        self.k_list = sorted(k_list)
        self.max_mae = max_mae
        self.metric_name = "Accuracy"

    def _calculate_metrics_for_users(
            self,
            y_true: List[List[float]],
            y_predicted: List[List[float]],
            k: int,
    ) -> float:
        user_scores = [
            accuracy_k(
                y_true=true,
                y_predicted=predicted,
                max_mae=self.max_mae,
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
    ) -> Dict[str, dict]:
        results = {}
        for k in self.k_list:
            score = self._calculate_metrics_for_users(
                y_true=y_true,
                y_predicted=y_predicted,
                k=k,
            )
            if k:
                results[f"{self.metric_name}@{k}"] = score
            else:
                results[f"{self.metric_name}"] = score
        return {model_name: results}

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
        result_df = pd.DataFrame(model_results).T
        result_df.index.name = "Model"
        return result_df