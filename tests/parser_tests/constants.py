from datetime import date

EXPECTED_RESULT_1 = [
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250623162000.xls?r=8208', date(2025, 6, 23)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250620162000.xls?r=1385', date(2025, 6, 20)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250619162000.xls?r=9057', date(2025, 6, 19)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250618162000.xls?r=5557', date(2025, 6, 18)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250617162000.xls?r=6850', date(2025, 6, 17)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250616162000.xls?r=3968', date(2025, 6, 16)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250611162000.xls?r=6466', date(2025, 6, 11)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250610162000.xls?r=3965', date(2025, 6, 10)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250609162000.xls?r=1716', date(2025, 6, 9)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250606162000.xls?r=6791', date(2025, 6, 6))
]
EXPECTED_RESULT_2 = [
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250618162000.xls?r=5557', date(2025, 6, 18)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250617162000.xls?r=6850', date(2025, 6, 17)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250616162000.xls?r=3968', date(2025, 6, 16)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250611162000.xls?r=6466', date(2025, 6, 11))
]
EXPECTED_RESULT_3 = [
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250507162000.xls?r=9369', date(2025, 5, 7)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250512162000.xls?r=9601', date(2025, 5, 12)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250513162000.xls?r=2422', date(2025, 5, 13)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250514162000.xls?r=1279', date(2025, 5, 14)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250515162000.xls?r=6749', date(2025, 5, 15)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250516162000.xls?r=8773', date(2025, 5, 16)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250519162000.xls?r=8818', date(2025, 5, 19)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250520162000.xls?r=4595', date(2025, 5, 20)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250521162000.xls?r=2942', date(2025, 5, 21)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250522162000.xls?r=7609', date(2025, 5, 22)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250523162000.xls?r=3787', date(2025, 5, 23)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250526162000.xls?r=4716', date(2025, 5, 26)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250527162000.xls?r=7000', date(2025, 5, 27)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250528162000.xls?r=4763', date(2025, 5, 28)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250529162000.xls?r=7429', date(2025, 5, 29)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250530162000.xls?r=5534', date(2025, 5, 30)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250602162000.xls?r=7604', date(2025, 6, 2)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250603162000.xls?r=3874', date(2025, 6, 3)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250604162000.xls?r=8559', date(2025, 6, 4)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250605162000.xls?r=7980', date(2025, 6, 5)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250606162000.xls?r=6791', date(2025, 6, 6)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250609162000.xls?r=1716', date(2025, 6, 9)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250610162000.xls?r=3965', date(2025, 6, 10)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250611162000.xls?r=6466', date(2025, 6, 11)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250616162000.xls?r=3968', date(2025, 6, 16)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250617162000.xls?r=6850', date(2025, 6, 17)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250618162000.xls?r=5557', date(2025, 6, 18)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250619162000.xls?r=9057', date(2025, 6, 19)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250620162000.xls?r=1385', date(2025, 6, 20)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250623162000.xls?r=8208', date(2025, 6, 23))
]
EXPECTED_RESULT_4 = [
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250523162000.xls?r=3787', date(2025, 5, 23)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250526162000.xls?r=4716', date(2025, 5, 26)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250527162000.xls?r=7000', date(2025, 5, 27)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250528162000.xls?r=4763', date(2025, 5, 28)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250529162000.xls?r=7429', date(2025, 5, 29)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250530162000.xls?r=5534', date(2025, 5, 30)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250602162000.xls?r=7604', date(2025, 6, 2))
]
EXPECTED_RESULT_5 = [
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250610162000.xls?r=3965', date(2025, 6, 10)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250611162000.xls?r=6466', date(2025, 6, 11)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250616162000.xls?r=3968', date(2025, 6, 16)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250617162000.xls?r=6850', date(2025, 6, 17)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250618162000.xls?r=5557', date(2025, 6, 18)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250619162000.xls?r=9057', date(2025, 6, 19)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250620162000.xls?r=1385', date(2025, 6, 20)),
    ('https://spimex.com/upload/reports/oil_xls/oil_xls_20250623162000.xls?r=8208', date(2025, 6, 23))
]
