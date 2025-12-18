import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import pandas as pd
import dask.dataframe as dd

from src.pipelines.base_model_pipeline import BaseModelPipeline
from src.utils.config import settings
from src.baseline.baseline import Baseline

logger = logging.getLogger(__name__)


class BaselinePipeline(BaseModelPipeline):
    _METHOD_DISPATCH: Dict[str, Callable] = {
        "popularity": Baseline.popularity_baseline,
        "mean_rating": Baseline.mean_rating_baseline,
        "bayesian": Baseline.bayesian_mean_baseline,
        "recent": Baseline.recent_popularity_baseline,
        "random": Baseline.random_baseline,
    }


    def __init__(
            self,
            method: str = "popularity",
            model_name: Optional[str] = None,
            model_params: Optional[Dict[str, Any]] = None,
            movies_df: Optional[pd.DataFrame] = None,
            ratings_df: Optional[pd.DataFrame] = None
    ):
        if method not in self._METHOD_DISPATCH:
            raise ValueError(
                f"Unknown baseline method '{method}'. "
                f"Available: {list(self._METHOD_DISPATCH.keys())}"
            )

        self.method = method
        self.movies_df = movies_df
        self.ratings_df = ratings_df

        if model_name is None:
            model_name = f"Baseline_{method.capitalize()}"

        default_params: Dict[str, Any] = {
            "n_recommendations": 100
        }


        method_params = {
            'mean_rating': {'min_n_ratings': 25},
            'bayesian': {'m': 25},
            'recent': {'window_days': 180},
            'random': {'random_state': 42}
        }

        if method in method_params:
            default_params.update(method_params[method])


        if model_params:
            default_params.update(model_params)

        super().__init__(
            model_name=model_name,
            model_params=default_params
        )


    def _create_model(self):
        return self

    def _fit_model(self, train_data: dd.DataFrame):
        logger.info(f"Подготовка бейзлайна '{self.method}'")

        self.ratings_df = train_data.compute()

        if self.movies_df is None:
            self.movies_df = pd.DataFrame({
                "movieId": self.ratings_df["movieId"].unique()
            })

    def _recommend_impl(self, user_id: int, items: List[int]) -> List[int]:
        if self.movies_df is None or self.ratings_df is None:
            raise RuntimeError("Pipeline is not fitted. Call fit() first.")

        recommend_func = self._METHOD_DISPATCH[self.method]

        recommendations = recommend_func(
            self.movies_df,
            self.ratings_df,
            user_id,
            **self.model_params
        )

        if items:
            allowed = set(items)
            recommendations = [i for i in recommendations if i in allowed]

        return recommendations

    def train(self, train_data: dd.DataFrame, force_retrain: bool = False):
        logger.info(f"Training baseline pipeline '{self.method}'")

        self._fit_model(train_data)
        self.is_trained = True
        self.model = self

        return self
