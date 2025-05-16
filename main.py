from core.utils import extract_data_from_xls, input_dates, parse_all_pages, \
                       save_data_to_db

if __name__ == '__main__':

    save_data_to_db(extract_data_from_xls(parse_all_pages(*input_dates())))
