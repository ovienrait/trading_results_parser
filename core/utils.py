from datetime import date, datetime
from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import select
from tqdm import tqdm

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


def parse_all_pages(
    cutoff_start_date: date, cutoff_end_date: date
) -> List[Tuple[str, date]]:
    """Парсит все страницы с результатами торгов за указанный период."""

    all_xls_links: List[Tuple[str, date]] = []
    last_page_number = get_last_page_number()

    logger.info(
        f'Поиск записей за период с {cutoff_start_date} по {cutoff_end_date}.'
    )

    with tqdm(
        desc='Парсинг страниц', unit='стр', leave=False, initial=1
    ) as pbar:
        for page_number in range(1, last_page_number + 1):
            url = f'{BASE_URL}{RELATIVE_URL}?page=page-{page_number}'
            xls_links, stop_flag = get_xls_links_from_page(
                url, cutoff_start_date, cutoff_end_date, page_number
            )

            pbar.update(1)
            all_xls_links.extend(xls_links)

            if stop_flag:
                break

        logger.info(f'Всего ссылок на XLS-файлы найдено: {len(all_xls_links)}')
        return list(reversed(all_xls_links))


def get_xls_links_from_page(
    url: str, cutoff_start_date: date, cutoff_end_date: date, page_number: int
) -> Tuple[List[Tuple[str, date]], bool]:
    """Получает ссылки на XLS-файлы с указанной страницы."""

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    xls_links: List[Tuple[str, date]] = []
    stop_flag = False
    items = soup.find_all('div', attrs={'class': 'accordeon-inner__wrap-item'})
    for item in items:
        xls_url = item.find(
            'div', attrs={'class': 'accordeon-inner__header'}
        ).find('a')['href']
        if 'reports' in xls_url:
            xls_date_str = item.find('span').text
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


def extract_data_from_xls(
    all_xls_links: List[Tuple[str, date]]
) -> List[SpimexTradingResultSchema]:
    """Извлекает данные из XLS-файлов."""

    results: List[SpimexTradingResultSchema] = []
    for xls_link, xls_date in tqdm(
        all_xls_links, desc='Загрузка и парсинг XLS', unit='файл'
    ):
        response = requests.get(xls_link)
        sheet = pd.read_excel(
            BytesIO(response.content), header=None, engine='xlrd'
        )
        header_index: Optional[int] = None
        for i in range(len(sheet)):
            row = sheet.iloc[i]
            if row.astype(str).str.contains(TABLE_NAME, case=False).any():
                header_index = i + 1
                break

        if header_index is None:
            continue

        table_rows = []
        for k in range(header_index + 2, len(sheet)):
            row_data = sheet.iloc[k]
            if (
                row_data.astype(str).str.contains(TABLE_END, case=False).any()
                or row_data.isnull().all()
            ):
                break
            table_rows.append(row_data)

        if table_rows:
            df = pd.DataFrame(table_rows)
            df.columns = sheet.iloc[header_index]
            df[HEADERS[7][1]] = xls_date
            df[HEADERS[6][0]] = pd.to_numeric(
                df[HEADERS[6][0]], errors='coerce'
            )
            df_filtered = df[
                df[HEADERS[6][0]].fillna(0).astype(int) > 0
            ].copy()
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

            for record in df_filtered.to_dict(orient='records'):
                obj = SpimexTradingResultSchema(**record)
                results.append(obj)

            logger.info(
                f'Файл бюллетеня от {xls_date} обработан. '
                f'Найдено записей: {len(df_filtered)}.'
            )

    logger.info(f'Всего записей: {len(results)}')
    return results


async def save_data_to_db_async(
    results: List[SpimexTradingResultSchema], batch_size: int = 1000
) -> None:
    async with async_session() as session:
        try:
            existing_dates = {
                row[0] for row in (
                    await session.execute(select(SpimexTradingResult.date))
                ).all()
            }

            new_records = [
                SpimexTradingResult(
                    **record.model_dump(exclude={'created_on', 'updated_on'})
                )
                for record in results
                if record.date not in existing_dates
            ]

            for i in range(0, len(new_records), batch_size):
                batch = new_records[i:i + batch_size]
                session.add_all(batch)
                await session.commit()

            if new_records:
                logger.info(f'Добавлено новых записей: {len(new_records)}')
            else:
                logger.info('Нет новых записей для добавления.')

        except Exception:
            await session.rollback()
            logger.info('Ошибка при сохранении данных в БД.')
