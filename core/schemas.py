from typing import Optional
from pydantic import BaseModel, Field, model_validator
from datetime import date


class SpimexTradingResultSchema(BaseModel):
    exchange_product_id: str = Field(..., alias='Код\nИнструмента')
    exchange_product_name: str = Field(..., alias='Наименование\nИнструмента')
    delivery_basis_name: str = Field(..., alias='Базис\nпоставки')
    volume: int = Field(..., alias='Объем\nДоговоров\nв единицах\nизмерения')
    total: int = Field(..., alias='Обьем\nДоговоров,\nруб.')
    count: int = Field(..., alias='Количество\nДоговоров,\nшт.')
    date: date

    oil_id: Optional[str] = None
    delivery_basis_id: Optional[str] = None
    delivery_type_id: Optional[str] = None

    @model_validator(mode='after')
    def compute_ids(cls, model):
        ep_id = model.exchange_product_id
        model.oil_id = ep_id[:4]
        model.delivery_basis_id = ep_id[4:7]
        model.delivery_type_id = ep_id[-1]
        return model
