import logging
from typing import List, Optional, Dict, Any, Union
from pathlib import Path

import dask.dataframe as dd

from src.pipelines.base_model_pipeline import BaseModelPipeline
from src.ml_models.item_based_cf.model import MLItemBasedCFSimple

logger = logging.getLogger(__name__)


class ItemBasedPipeline(BaseModelPipeline):
    def __init__(
            self,
            model_name: str = "ItemBased",
            model_params: Optional[Dict[str, Any]] = None,
            models_dir: Optional[Union[str, Path]] = None,
            overwrite_model: bool = False
    ):
        default_params: Dict[str, Any] = {}

        if model_params:
            default_params.update(model_params)

        super().__init__(
            model_name=model_name,
            model_params=default_params,
            models_dir=models_dir,
            overwrite_model=overwrite_model
        )

    def _create_model(self) -> MLItemBasedCFSimple:
        logger.info("Создание ItemBased модели")
        return MLItemBasedCFSimple()

    def _fit_model(self, train_data: dd.DataFrame):
        logger.info(f"Обучение ItemBased модели...")
        self.model.fit(train_data)

    def _recommend_impl(
            self,
            user_id: int,
            items: List[int] | None = None
    ) -> List[int]:
        return self.model.getting_recommended_movies(
            user_id=user_id,
            movies_list=items
        )

    def predict_rating(self, user_id: int, item_id: int) -> float:
        if not self.is_trained:
            raise ValueError("Модель не обучена!")

        return self.model.predict(user_id, item_id).est