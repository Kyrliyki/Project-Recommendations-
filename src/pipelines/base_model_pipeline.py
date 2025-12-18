import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union, Tuple
import pickle
import logging
from pathlib import Path
from datetime import datetime
import shutil

import pandas as pd
import dask.dataframe as dd

logger = logging.getLogger(__name__)


class BaseModelPipeline(ABC):

    def __init__(
            self,
            model_name: str,
            model_params: Optional[Dict[str, Any]] = None,
            models_dir: Union[str, Path] = "artifacts/models",
            overwrite_model: bool = False
    ):
        self.model_name = model_name
        self.model_params = model_params or {}
        self.model = None
        self.is_trained = False

        self.models_base_dir = Path(models_dir)

        self.model_dir = self.models_base_dir / model_name

        if overwrite_model and self.model_dir.exists():
            shutil.rmtree(self.model_dir)

        self.model_dir.mkdir(parents=True, exist_ok=True)

        self.model_path = self.model_dir / f"{model_name}.pkl"
        self.info_path = self.model_dir / f"{model_name}_info.json"

    @abstractmethod
    def _create_model(self):
        pass

    @abstractmethod
    def _fit_model(self, train_data: dd.DataFrame):
        pass

    @abstractmethod
    def _recommend_impl(self, user_id: int, items: List[int] | None = None) -> List[int]:
        pass



    def train(self, train_data: dd.DataFrame, force_retrain: bool = False):
        logger.info(f"Обучение модели '{self.model_name}'")

        if self.model_path.exists() and not force_retrain:
            try:
                self._load_model()
                logger.info("Загрузка существующей модели")
                return self
            except Exception:
                logger.warning("Неудача загрузки модели")

        self.model = self._create_model()
        self._fit_model(train_data)
        self.is_trained = True

        self._save_model()
        self._save_info(train_data)

        logger.info("Модель обучена и сохранена")
        return self

    def recommend(
            self,
            user_id: int,
            items: List[int] | None = None,
            k: Optional[int] = None
    ) -> List[int]:

        if not self.is_trained:
            raise RuntimeError(f"Модель '{self.model_name}' не обучена!")

        recs = self._recommend_impl(user_id, items)
        return recs[:k] if k else recs

    def collect_recommendations(
            self,
            test_cases: List[Tuple[int, List[int], List[int]]],
            k: Optional[int] = None
    ) -> Tuple[List[List[int]], List[List[int]]]:
        """Return: all_recommendations, all_relevant"""
        if not self.is_trained:
            raise RuntimeError(f"Модель '{self.model_name}' не обучена!")

        all_recommendations: List[List[int]] = []
        all_relevant: List[List[int]] = []

        for user_id, candidates, relevant in test_cases:
            recommendations = self.recommend(
                user_id=user_id,
                items=candidates,
                k=k
            )

            all_recommendations.append(recommendations)
            all_relevant.append(relevant)

        return all_recommendations, all_relevant

    def _save_model(self):
        with open(self.model_path, "wb") as f:
            pickle.dump(self.model, f)

    def _load_model(self):
        with open(self.model_path, "rb") as f:
            self.model = pickle.load(f)
        self.is_trained = True

    def _save_info(self, train_data: dd.DataFrame):
        info = {
            "model_name": self.model_name,
            "params": self.model_params,
            "trained_at": datetime.now().isoformat(),
            "train_rows": len(train_data),
            "columns": list(train_data.columns),
        }
        with open(self.info_path, "w") as f:
            json.dump(info, f, indent=2)

    def cleanup(self):
        if self.model_dir.exists():
            shutil.rmtree(self.model_dir)
        self.model = None
        self.is_trained = False