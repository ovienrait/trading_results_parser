import asyncio
from core.utils import extract_data_from_xls, input_dates, parse_all_pages, \
                       save_data_to_db_async


async def main():
    start_date, end_date = input_dates()
    links = parse_all_pages(start_date, end_date)
    results = extract_data_from_xls(links)
    await save_data_to_db_async(results)


if __name__ == '__main__':
    asyncio.run(main())
