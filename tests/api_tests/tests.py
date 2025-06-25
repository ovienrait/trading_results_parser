from datetime import date

import pytest

from api.routers import get_dynamics as cached_get_dynamics
from api.routers import get_trading_results as cached_get_trading_results
from api.routers import last_trading_dates as cached_last_trading_dates


@pytest.mark.asyncio
async def test_last_trading_dates(async_session, db_object):

    async_session.add_all([
        db_object(date=date(2024, 6, 20)),
        db_object(date=date(2024, 6, 19)),
        db_object(date=date(2024, 6, 18)),
    ])
    await async_session.commit()

    last_trading_dates = cached_last_trading_dates.__wrapped__

    result = await last_trading_dates(limit=2, session=async_session)

    assert result == [date(2024, 6, 20), date(2024, 6, 19)]


@pytest.mark.asyncio
async def test_get_dynamics(async_session, db_object):

    async_session.add_all(
        [db_object() for _ in range(5)] + [db_object(oil_id='OIL2'), db_object(date=date(2024, 6, 24))])
    await async_session.commit()

    get_dynamics = cached_get_dynamics.__wrapped__

    result = await get_dynamics(
        start_date=date(2024, 6, 20),
        end_date=date(2024, 6, 21),
        oil_id='OIL1',
        session=async_session
    )

    assert len(result) == 5
    for obj in result:
        assert obj.oil_id == 'OIL1'


@pytest.mark.asyncio
async def test_get_trading_results(async_session, db_object):

    async_session.add_all(
        [db_object() for _ in range(3)] + [db_object(oil_id='OIL2'), db_object(date=date(2024, 6, 20))])
    await async_session.commit()

    get_trading_results = cached_get_trading_results.__wrapped__

    result = await get_trading_results(oil_id='OIL1', session=async_session)

    assert len(result) == 3
    for obj in result:
        assert obj.oil_id == 'OIL1'
