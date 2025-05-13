from datetime import date, datetime
from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from core.database import Session
from core.logger_setup import setup_logger
from core.models import SpimexTradingResult

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


def parse_all_pages(
    cutoff_start_date: date, cutoff_end_date: date, page_number: int = 1
) -> List[Tuple[str, date]]:
    """Парсит все страницы с результатами торгов за указанный период."""

    all_xls_links: List[Tuple[str, date]] = []

    logger.info(
        f'Поиск записей за период с {cutoff_start_date} по {cutoff_end_date}.'
    )

    with tqdm(
        desc='Парсинг страниц', unit='стр', leave=False, initial=1
    ) as pbar:
        while True:
            url = f'{BASE_URL}{RELATIVE_URL}?page=page-{page_number}'
            xls_links, stop_flag = get_xls_links_from_page(
                url, cutoff_start_date, cutoff_end_date, page_number
            )

            pbar.update(1)
            all_xls_links.extend(xls_links)
            page_number += 1

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
) -> pd.DataFrame:
    """Извлекает данные из XLS-файлов и возвращает их в виде DataFrame."""

    total_dataframe: List[pd.DataFrame] = []
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
            total_dataframe.append(df)

        logger.info(
            f'Файл бюллетеня от {xls_date} обработан. '
            f'Найдено {len(table_rows)} новых записей.'
        )

    if total_dataframe:
        all_data = pd.concat(total_dataframe, ignore_index=True)
        all_data.columns = all_data.columns.str.strip()
        all_data[HEADERS[6][0]] = pd.to_numeric(
            all_data[HEADERS[6][0]], errors='coerce'
        )
        filtered_data = all_data[
            all_data[HEADERS[6][0]].fillna(0).astype(int) > 0
        ]
        filtered_data = filtered_data.copy()
        filtered_data[HEADERS[6][0]] = filtered_data[HEADERS[6][0]].astype(int)
        filtered_data = filtered_data.reset_index(drop=True)
        final_dataframe = filtered_data[
            [
                HEADERS[1][0],
                HEADERS[2][0],
                HEADERS[3][0],
                HEADERS[4][0],
                HEADERS[5][0],
                HEADERS[6][0],
                HEADERS[7][1],
            ]
        ].rename(
            columns={
                HEADERS[1][0]: HEADERS[1][1],
                HEADERS[2][0]: HEADERS[2][1],
                HEADERS[3][0]: HEADERS[3][1],
                HEADERS[4][0]: HEADERS[4][1],
                HEADERS[5][0]: HEADERS[5][1],
                HEADERS[6][0]: HEADERS[6][1],
            }
        )

        logger.info(f'Всего записей после фильтрации: {len(final_dataframe)}')
        return final_dataframe

    return pd.DataFrame()


def check_field_for_update(obj: object, field: str, new_value: object) -> int:
    """Проверяет, нужно ли обновлять поле в объекте."""

    if getattr(obj, field) != new_value:
        setattr(obj, field, new_value)
        return 1
    return 0


def save_data_to_db(final_dataframe: pd.DataFrame) -> None:
    """Сохраняет данные в БД, обновляя существующие записи."""

    new_records: List[SpimexTradingResult] = []
    changed_records = 0

    with Session() as session:
        try:
            for _, row in tqdm(
                final_dataframe.iterrows(),
                total=len(final_dataframe),
                desc='Сохранение в БД',
                unit='запись',
            ):
                existing = (
                    session.query(SpimexTradingResult)
                    .filter_by(
                        exchange_product_id=row[HEADERS[1][1]],
                        date=row[HEADERS[7][1]]
                    )
                    .first()
                )

                if existing:
                    counter = 0
                    counter += check_field_for_update(
                        existing, HEADERS[2][1], row[HEADERS[2][1]]
                    )
                    counter += check_field_for_update(
                        existing, HEADERS[3][1], row[HEADERS[3][1]]
                    )
                    counter += check_field_for_update(
                        existing, HEADERS[4][1], int(row[HEADERS[4][1]])
                    )
                    counter += check_field_for_update(
                        existing, HEADERS[5][1], int(row[HEADERS[5][1]])
                    )
                    counter += check_field_for_update(
                        existing, HEADERS[6][1], int(row[HEADERS[6][1]])
                    )

                    if counter > 0:
                        existing.updated_on = datetime.now().replace(
                            microsecond=0
                        )
                        changed_records += 1

                else:
                    record = SpimexTradingResult(
                        exchange_product_id=row[HEADERS[1][1]],
                        exchange_product_name=row[HEADERS[2][1]],
                        oil_id=row[HEADERS[1][1]][:4],
                        delivery_basis_id=row[HEADERS[1][1]][4:7],
                        delivery_basis_name=row[HEADERS[3][1]],
                        delivery_type_id=row[HEADERS[1][1]][-1],
                        volume=int(row[HEADERS[4][1]]),
                        total=int(row[HEADERS[5][1]]),
                        count=int(row[HEADERS[6][1]]),
                        date=row[HEADERS[7][1]],
                    )
                    new_records.append(record)

            session.add_all(new_records)
            session.commit()

            logger.info(
                f'Новых записей добавлено: {len(new_records)}. '
                f'Записей изменено: {changed_records}.'
            )

        except Exception:
            session.rollback()
            logger.info('Ошибка при сохранении данных в БД.')
