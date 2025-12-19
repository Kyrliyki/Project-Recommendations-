import random
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any
import logging

import dask.dataframe as dd

logger = logging.getLogger(__name__)


class BaseEvaluationProtocol(ABC):
    def __init__(self, name: str, random_seed: int = 21):
        self.name = name
        self.rng = random.Random(random_seed)

    @abstractmethod
    def prepare_test_cases(
        self,
        train: dd.DataFrame,
        validation: dd.DataFrame
    ) -> List[Tuple[int, List[int], List[int]]]:
        pass