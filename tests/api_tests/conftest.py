from datetime import date

import pytest
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)

from core.models import Base, SpimexTradingResult

DATABASE_URL = 'sqlite+aiosqlite:///:memory:'


@pytest.fixture
async def async_session():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        yield session

    await engine.dispose()


@pytest.fixture
def db_object():
    def _db_object(**kwargs):
        return SpimexTradingResult(
            exchange_product_id=kwargs.get('exchange_product_id', 'ID1'),
            exchange_product_name=kwargs.get('exchange_product_name', 'Product A'),
            oil_id=kwargs.get('oil_id', 'OIL1'),
            delivery_basis_id=kwargs.get('delivery_basis_id', 'DB1'),
            delivery_basis_name=kwargs.get('delivery_basis_name', 'Basis A'),
            delivery_type_id=kwargs.get('delivery_type_id', 'T'),
            volume=kwargs.get('volume', 100),
            total=kwargs.get('total', 1000),
            count=kwargs.get('count', 1),
            date=kwargs.get('date', date(2024, 6, 21)),
        )
    return _db_object
