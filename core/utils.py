from datetime import date, datetime
from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd
import requests
import asyncio
import aiohttp
from typing import Callable
from aioprocessing import AioPool
import multiprocessing
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup
from pydantic import TypeAdapter
from sqlalchemy import select

from core.database import async_session
from core.logger_setup import setup_logger
from core.models import SpimexTradingResult
from core.schemas import SpimexTradingResultSchema

BASE_URL = 'https://spimex.com'
RELATIVE_URL = '/markets/oil_products/trades/results/'
TABLE_NAME = 'Единица измерения: Метрическая тонна'
TABLE_END = 'Итого:'
HEADERS = {
    1: ('Код\nИнструмента', 'exchange_product_id'),
    2: ('Наименование\nИнструмента', 'exchange_product_name'),
    3: ('Базис\nпоставки', 'delivery_basis_name'),
    4: ('Объем\nДоговоров\nв единицах\nизмерения', 'volume'),
    5: ('Обьем\nДоговоров,\nруб.', 'total'),
    6: ('Количество\nДоговоров,\nшт.', 'count'),
    7: ('', 'date'),
}

logger = setup_logger()


def input_dates() -> Tuple[date, date]:
    """Запрашивает у пользователя даты начала и окончания периода."""

    while True:
        try:
            start_input = input(
                'Введите дату начала периода в формате ДД.ММ.ГГГГ '
                '(например: 04.05.2025): '
            )
            cutoff_start_date = datetime.strptime(
                start_input, '%d.%m.%Y').date()

            if cutoff_start_date > datetime.now().date():
                print('Дата начала периода не может быть позже текущей даты.')
                continue

            end_input = input(
                'Введите дату окончания периода в формате ДД.ММ.ГГГГ '
                '(например: 09.05.2025): '
            )
            cutoff_end_date = datetime.strptime(end_input, '%d.%m.%Y').date()

            if cutoff_start_date > cutoff_end_date:
                print(
                    'Дата начала периода не может быть позже даты '
                    'окончания периода. Повторите ввод.'
                )
                continue

            return cutoff_start_date, cutoff_end_date

        except ValueError:
            print('Неверный формат даты. Повторите ввод.')


def get_last_page_number() -> int:
    """Получает номер последней страницы с результатами торгов."""

    url = f'{BASE_URL}{RELATIVE_URL}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    pagination = soup.find('div', attrs={'class': 'bx-pagination-container'})

    page_numbers = []
    for li in pagination.find_all('li'):
        span = li.find('span')
        if span and span.text.strip().isdigit():
            page_numbers.append(int(span.text.strip()))

    return max(page_numbers)


async def find_page_bounds_binary(
    session: aiohttp.ClientSession, total_pages: int,
    cutoff_start_date: date, cutoff_end_date: date,
) -> Tuple[int, int]:
    """Получает начальную и конечную страницу для парсинга."""

    async def get_page_dates(page_number: int) -> List[date]:
        url = f'{BASE_URL}{RELATIVE_URL}?page=page-{page_number}'
        try:
            async with session.get(url) as response:
                html = await response.text()
        except Exception as e:
            logger.error(f'Ошибка при загрузке страницы {url}: {e}')
            return []

        soup = BeautifulSoup(html, 'lxml')
        items = soup.find_all(
            'div', attrs={'class': 'accordeon-inner__wrap-item'})
        dates = []
        for item in items:
            try:
                date_str = item.find('span').text.strip()
                parsed_date = datetime.strptime(date_str, '%d.%m.%Y').date()
                dates.append(parsed_date)
            except Exception:
                continue
        return dates

    async def binary_search(
        left: int, right: int, check_condition: Callable[[List[date]], bool],
        find_first: bool
    ) -> int:

        result = 0
        while left <= right:
            mid = (left + right) // 2
            dates = await get_page_dates(mid)

            if check_condition(dates):
                result = mid
                if find_first:
                    right = mid - 1
                else:
                    left = mid + 1
            else:
                if find_first:
                    left = mid + 1
                else:
                    right = mid - 1
        return result

    start_page = await binary_search(
        1, total_pages,
        lambda dates: any(d <= cutoff_end_date for d in dates),
        find_first=True)

    end_page = await binary_search(
        start_page, total_pages,
        lambda dates: any(d >= cutoff_start_date for d in dates),
        find_first=False)

    return start_page, end_page


async def parse_all_pages(
    cutoff_start_date: date, cutoff_end_date: date
) -> List[Tuple[str, date]]:
    """Асинхронный парсинг всех страниц с результатами торгов."""

    last_page_number = get_last_page_number()
    stop_event = asyncio.Event()
    semaphore = asyncio.Semaphore(30)

    logger.info(
        f'Поиск записей за период с {cutoff_start_date} по {cutoff_end_date}.'
    )

    async with aiohttp.ClientSession() as session:
        start_page, end_page = await find_page_bounds_binary(
            session, last_page_number, cutoff_start_date, cutoff_end_date)

        logger.info(f'Обрабатываем страницы с {start_page} по {end_page}.')

        async def fetch(page_number: int):
            if stop_event.is_set():
                return []

            url = f'{BASE_URL}{RELATIVE_URL}?page=page-{page_number}'

            async with semaphore:
                xls_links, stop_flag = await get_xls_links_from_page(
                    session, url, cutoff_start_date, cutoff_end_date,
                    page_number)

                if stop_flag:
                    stop_event.set()

                return xls_links

        tasks = [asyncio.create_task(
            fetch(page)
        ) for page in range(start_page, end_page + 1)]

        all_results = []
        for completed in tqdm_asyncio.as_completed(
            tasks, total=len(tasks), desc='Парсинг страниц', unit='стр'
        ):
            links = await completed
            all_results.extend(links)

        logger.info(f'Всего ссылок на XLS-файлы найдено: {len(all_results)}')
        return list(reversed(all_results))


async def get_xls_links_from_page(
    session: aiohttp.ClientSession,
    url: str,
    cutoff_start_date: date,
    cutoff_end_date: date,
    page_number: int
) -> Tuple[List[Tuple[str, date]], bool]:
    """Асинхронно получает ссылки на XLS-файлы с указанной страницы."""

    try:
        async with session.get(url) as response:
            html = await response.text()
    except Exception as e:
        logger.error(f'Ошибка при загрузке страницы {url}: {e}')
        return [], False

    soup = BeautifulSoup(html, 'lxml')
    xls_links: List[Tuple[str, date]] = []
    stop_flag = False

    items = soup.find_all('div', attrs={'class': 'accordeon-inner__wrap-item'})
    for item in items:
        xls_tag = item.find('div', class_='accordeon-inner__header').find('a')
        if not xls_tag or 'reports' not in xls_tag['href']:
            continue

        xls_url = xls_tag['href']
        xls_date_str = item.find('span').text.strip()
        xls_date = datetime.strptime(xls_date_str, '%d.%m.%Y').date()

        if cutoff_end_date >= xls_date >= cutoff_start_date:
            full_xls_url = BASE_URL + xls_url
            xls_links.append((full_xls_url, xls_date))
        elif xls_date < cutoff_start_date:
            stop_flag = True
            break

    if xls_links:
        logger.info(
            f'Страница {page_number} обработана. '
            f'Найдено {len(xls_links)} ссылок.'
        )

    return xls_links, stop_flag


def sync_parse_xls(content: bytes, xls_date: date) -> list[dict]:
    """Синхронно парсит XLS-файл и возвращает данные в виде списка словарей."""

    sheet = pd.read_excel(BytesIO(content), header=None, engine='xlrd')
    header_index: Optional[int] = None
    for i in range(len(sheet)):
        row = sheet.iloc[i]
        if row.astype(str).str.contains(TABLE_NAME, case=False).any():
            header_index = i + 1
            break

    if header_index is None:
        return []

    table_rows = []
    for k in range(header_index + 2, len(sheet)):
        row_data = sheet.iloc[k]
        if (
            row_data.astype(str).str.contains(TABLE_END, case=False).any()
            or row_data.isnull().all()
        ):
            break
        table_rows.append(row_data)

    if not table_rows:
        return []

    df = pd.DataFrame(table_rows)
    df.columns = sheet.iloc[header_index]
    df[HEADERS[7][1]] = xls_date
    df[HEADERS[6][0]] = pd.to_numeric(df[HEADERS[6][0]], errors='coerce')
    df_filtered = df[df[HEADERS[6][0]].fillna(0).astype(int) > 0].copy()
    df_filtered[HEADERS[6][0]] = df_filtered[HEADERS[6][0]].astype(int)
    df_filtered = df_filtered.reset_index(drop=True)
    df_filtered = df_filtered[
        [
            HEADERS[1][0],
            HEADERS[2][0],
            HEADERS[3][0],
            HEADERS[4][0],
            HEADERS[5][0],
            HEADERS[6][0],
            HEADERS[7][1],
        ]
    ]
    return df_filtered.to_dict(orient='records')


async def extract_data_from_xls(
    all_xls_links: List[Tuple[str, date]]
) -> List[SpimexTradingResultSchema]:
    """Асинхронно извлекает данные из XLS-файлов по ссылкам."""

    results: List[SpimexTradingResultSchema] = []
    adapter = TypeAdapter(list[SpimexTradingResultSchema])
    semaphore = asyncio.Semaphore(100)

    async with aiohttp.ClientSession() as session:
        pool = AioPool(processes=min(4, multiprocessing.cpu_count()))
        try:
            async def fetch_and_parse(xls_link: str, xls_date: date):
                async with semaphore:
                    try:
                        async with session.get(xls_link) as response:
                            if response.status != 200:
                                logger.warning(
                                    f'Не удалось загрузить {xls_link}')
                                return []

                            content = await response.read()

                            raw_data = await pool.coro_apply(
                                sync_parse_xls, args=(content, xls_date))
                            if not raw_data:
                                return []

                            parsed = adapter.validate_python(raw_data)
                            logger.info(
                                f'Файл бюллетеня от {xls_date} обработан. '
                                f'Найдено записей: {len(parsed)}.'
                            )
                            return parsed

                    except Exception as e:
                        logger.error(f'Ошибка при обработке {xls_link}: {e}')
                        return []

            tasks = [
                asyncio.create_task(fetch_and_parse(link, xls_date))
                for link, xls_date in all_xls_links
            ]

            for task in tqdm_asyncio.as_completed(
                tasks, total=len(tasks), desc='Обработка XLS', unit='файл'
            ):
                data = await task
                results.extend(data)

        finally:
            pool.close()
            pool.join()

    logger.info(f'Всего записей: {len(results)}')
    return results


async def save_batch(
    batch: List[SpimexTradingResultSchema], semaphore: asyncio.Semaphore
) -> int:
    """Сохраняет батч данных в базу данных асинхронно."""

    async with semaphore:
        async with async_session() as session:
            try:
                models = [
                    SpimexTradingResult(
                        **record.model_dump(
                            exclude={'created_on', 'updated_on'})
                    )
                    for record in batch
                ]
                session.add_all(models)
                await session.flush()
                await session.commit()
                return len(models)

            except Exception as e:
                await session.rollback()
                logger.error(f'Ошибка при сохранении батча: {e}')
                return 0


async def save_data_to_db_async(
    results: List[SpimexTradingResultSchema], batch_size: int = 1000
) -> None:
    """Асинхронно сохраняет данные в базу данных."""

    async with async_session() as session:
        try:
            existing_dates = {
                row[0] for row in (
                    await session.execute(select(SpimexTradingResult.date))
                ).all()
            }
        except Exception as e:
            logger.error(f'Ошибка при проверке существующих дат: {e}')
            return

    new_records = [
        record for record in results if record.date not in existing_dates
    ]

    batches = [
        new_records[i:i + batch_size]
        for i in range(0, len(new_records), batch_size)
    ]

    semaphore = asyncio.Semaphore(10)

    tasks = await asyncio.gather(*[
        save_batch(batch, semaphore) for batch in batches
    ])

    total_saved = sum(tasks)
    if total_saved:
        logger.info(f'Добавлено новых записей: {total_saved}')
    else:
        logger.info('Нет новых записей для добавления.')
