from typing import List
from src.schemas import CustomModel


class DataModel(CustomModel):
    codigo_med: int
    stock_fin: int
    dates: List[str] # puede que cambie
    price: float