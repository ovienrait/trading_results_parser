from datetime import date
from unittest.mock import AsyncMock

import aiohttp
import pytest
import requests
from sqlalchemy import func, select

from core.models import SpimexTradingResult
from core.schemas import SpimexTradingResultSchema
from core.utils import (extract_data_from_xls, find_page_bounds_binary,
                        get_last_page_number, get_xls_links_from_page, input_dates,
                        parse_all_pages, save_batch, save_data_to_db_async, sync_parse_xls)
from tests.parser_tests.conftest import prepare_test_case

from .constants import (EXPECTED_RESULT_1, EXPECTED_RESULT_2, EXPECTED_RESULT_3,
                        EXPECTED_RESULT_4, EXPECTED_RESULT_5)


@pytest.mark.parametrize(
    'inputs, expected',
    [
        prepare_test_case(-5, -1, expected_days=(-5, -1)),
        prepare_test_case(2, -3, -1, expected_days=(-3, -1)),
        prepare_test_case(-1, -5, -5, -1, expected_days=(-5, -1)),
        prepare_test_case('2024-05-01', -4, -2, expected_days=(-4, -2)),
        prepare_test_case('abc', 3, -10, -1, expected_days=(-10, -1))
    ]
)
def test_input_dates(monkeypatch, inputs, expected):

    monkeypatch.setattr('builtins.input', lambda _: inputs.pop(0))
    result = input_dates()

    assert result == expected


def test_get_last_page_number(monkeypatch, mock_html_pages):

    html = mock_html_pages[1]
    monkeypatch.setattr(requests, 'get', lambda _: type('Resp', (), {'text': html}))
    result = get_last_page_number()

    assert result == 391


@pytest.mark.asyncio
async def test_find_page_bounds_binary(mock_session, mock_html_pages):

    async with mock_session(mock_html_pages) as session:
        start, end = await find_page_bounds_binary(
            session=session, total_pages=5,
            cutoff_start_date=date(2025, 4, 22), cutoff_end_date=date(2025, 6, 2)
        )

    assert start == 2
    assert end == 4


@pytest.mark.parametrize(
    'start_date, end_date, expected_result, expected_flag',
    [
        (date(2025, 6, 4), date(2025, 6, 24), EXPECTED_RESULT_1, False),
        (date(2025, 6, 11), date(2025, 6, 18), EXPECTED_RESULT_2, True),
    ]
)
@pytest.mark.asyncio
async def test_get_xls_links_from_page(mock_session, mock_html_pages,
                                       start_date, end_date, expected_result, expected_flag):

    links, stop_flag = await get_xls_links_from_page(
        session=mock_session(mock_html_pages),
        url='https://spimex.com/markets/oil_products/trades/results/?page=page-1',
        cutoff_start_date=start_date,
        cutoff_end_date=end_date,
        page_number=1
    )

    assert links == expected_result
    assert stop_flag == expected_flag


@pytest.mark.parametrize(
    'start_date, end_date, expected_result',
    [
        (date(2025, 5, 6), date(2025, 6, 24), EXPECTED_RESULT_3),
        (date(2025, 5, 23), date(2025, 6, 2), EXPECTED_RESULT_4),
        (date(2025, 6, 10), date(2025, 6, 24), EXPECTED_RESULT_5),
    ]
)
@pytest.mark.asyncio
async def test_parse_all_pages(monkeypatch, mock_session, mock_html_pages, start_date, end_date, expected_result):

    monkeypatch.setattr('core.utils.get_last_page_number', lambda: 3)
    monkeypatch.setattr('core.utils.find_page_bounds_binary', AsyncMock(return_value=(1, 3)))
    monkeypatch.setattr(aiohttp, 'ClientSession', lambda: mock_session(mock_html_pages))

    result = await parse_all_pages(
        cutoff_start_date=start_date,
        cutoff_end_date=end_date,
    )

    assert all(isinstance(item, tuple) for item in result)
    assert all('.xls' in item[0] for item in result)
    assert result == expected_result


def test_sync_parse_xls(mock_xls_files):

    file_path = mock_xls_files[0][0]
    content = file_path.read_bytes()
    xls_date = date(2025, 6, 10)

    result = sync_parse_xls(content, xls_date)

    assert isinstance(result, list)
    assert all(isinstance(row, dict) for row in result)
    for row in result:
        assert 'Код\nИнструмента' in row
        assert 'Наименование\nИнструмента' in row
        assert 'Базис\nпоставки' in row
        assert 'Объем\nДоговоров\nв единицах\nизмерения' in row
        assert 'Обьем\nДоговоров,\nруб.' in row
        assert 'Количество\nДоговоров,\nшт.' in row
        assert row['date'] == xls_date


@pytest.mark.asyncio
async def test_extract_data_from_xls(monkeypatch, mock_session, mock_xls_files):

    monkeypatch.setattr(aiohttp, 'ClientSession', lambda: mock_session(file_mode=True))
    results = await extract_data_from_xls(mock_xls_files)

    assert len(set(row.date for row in results)) == 3
    assert all(isinstance(row, SpimexTradingResultSchema) for row in results)


@pytest.mark.asyncio
async def test_save_batch(async_test_session, clean_db, sample_records, semaphore):

    batch = sample_records()
    saved_count = await save_batch(batch, semaphore, session_fabric=async_test_session)
    async with async_test_session() as session:
        result = await session.execute(select(func.count()).select_from(SpimexTradingResult))
        count = result.scalar()

    assert saved_count == len(batch)
    assert count == len(batch)


@pytest.mark.asyncio
async def test_save_data_to_db_async(async_test_session, clean_db, sample_records, semaphore):

    batch = sample_records(rec_count=1)
    saved_count = await save_batch(batch, semaphore, session_fabric=async_test_session)

    assert saved_count == 1

    await save_data_to_db_async(
        batch + sample_records(date=date(2023, 1, 1), rec_count=1), session_fabric=async_test_session, batch_size=2)

    async with async_test_session() as session:
        count_result = await session.execute(select(func.count()).select_from(SpimexTradingResult))
        final_count = count_result.scalar()

    assert final_count == 2
