from datetime import date
from typing import Optional

from pydantic import BaseModel


class SpimexTradingResultOut(BaseModel):

    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: Optional[int]
    total: Optional[int]
    count: Optional[int]
    date: date

    model_config = {
        'from_attributes': True
    }
