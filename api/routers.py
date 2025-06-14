from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi_cache.decorator import cache
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_async_session
from api.schemas import SpimexTradingResultOut
from core.models import SpimexTradingResult

router = APIRouter(prefix='/trading', tags=['Trading Results'])


@router.get(
    '/last-dates',
    summary='Получить список дат последних торговых дней'
)
@cache()
async def last_trading_dates(
    limit: int = Query(10, ge=1),
    session: AsyncSession = Depends(get_async_session),
):
    result = await session.execute(
        select(SpimexTradingResult.date)
        .distinct()
        .order_by(desc(SpimexTradingResult.date))
        .limit(limit)
    )
    result = [row[0] for row in result.fetchall()]
    return result


@router.get(
    '/dynamics',
    response_model=list[SpimexTradingResultOut],
    summary='Получить список торгов за заданный период'
)
@cache()
async def get_dynamics(
    start_date: date,
    end_date: date,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    session: AsyncSession = Depends(get_async_session),
):

    stmt = select(SpimexTradingResult).where(
        SpimexTradingResult.date.between(start_date, end_date)
    )

    stmt = apply_filters(stmt, oil_id, delivery_type_id, delivery_basis_id)
    stmt = stmt.order_by(SpimexTradingResult.date.desc())

    result = await session.execute(stmt)

    return result.scalars().all()


@router.get(
    '/latest-results',
    response_model=list[SpimexTradingResultOut],
    summary='Получить список последних торгов'
)
@cache()
async def get_trading_results(
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    session: AsyncSession = Depends(get_async_session),
):

    subquery = (
        select(func.max(SpimexTradingResult.date).label('max_date'))
        .scalar_subquery()
    )

    stmt = select(SpimexTradingResult).where(
        SpimexTradingResult.date == subquery
    )
    stmt = apply_filters(stmt, oil_id, delivery_type_id, delivery_basis_id)

    result = await session.execute(stmt)

    return result.scalars().all()


def apply_filters(
    stmt,
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None
):

    if oil_id:
        stmt = stmt.where(SpimexTradingResult.oil_id == oil_id)
    if delivery_type_id:
        stmt = stmt.where(
            SpimexTradingResult.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        stmt = stmt.where(
            SpimexTradingResult.delivery_basis_id == delivery_basis_id)
    return stmt
