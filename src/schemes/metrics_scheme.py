from pydantic import BaseModel


class MetricsScheme(BaseModel):
    RMSE: float
    Precision: float
    Recall: float
    MAP: float
    NDCG: float