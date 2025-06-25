import asyncio
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, SpimexTradingResult
from core.schemas import SpimexTradingResultSchema


def prepare_test_case(*input_days, expected_days):

    def date_format(days):
        return (datetime.now().date() + timedelta(days=days)).strftime('%d.%m.%Y') \
            if isinstance(days, int) else days
    inputs = list(map(date_format, input_days))
    expected = tuple(datetime.strptime(date_format(days), '%d.%m.%Y').date() for days in expected_days)
    return inputs, expected


@pytest.fixture
def mock_html_pages():

    return {
        1: (Path(__file__).parent / 'data/html/page_1.html').read_text(encoding='utf-8'),
        2: (Path(__file__).parent / 'data/html/page_2.html').read_text(encoding='utf-8'),
        3: (Path(__file__).parent / 'data/html/page_3.html').read_text(encoding='utf-8'),
        4: (Path(__file__).parent / 'data/html/page_4.html').read_text(encoding='utf-8'),
        5: (Path(__file__).parent / 'data/html/page_5.html').read_text(encoding='utf-8')
    }


@pytest.fixture
def mock_xls_files():

    return [
        (Path(__file__).parent / 'data/xls/oil_xls_20250610162000.xls', date(2025, 6, 10)),
        (Path(__file__).parent / 'data/xls/oil_xls_20250611162000.xls', date(2025, 6, 11)),
        (Path(__file__).parent / 'data/xls/oil_xls_20250616162000.xls', date(2025, 6, 16)),
    ]


@pytest.fixture
def mock_response():
    class MockResponse:
        def __init__(self, text_or_bytes, is_bytes=False):
            self._data = text_or_bytes
            self._is_bytes = is_bytes
            self.status = 200

        async def text(self):
            return self._data

        async def read(self):
            return self._data

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    return MockResponse


@pytest.fixture
def mock_session(mock_response):
    class MockSession:
        def __init__(self, html_by_page=None, file_mode=False):
            self.html_by_page = html_by_page or {}
            self.file_mode = file_mode

        def get(self, url):
            if self.file_mode:
                file_path = Path(url)
                content = file_path.read_bytes()
                return mock_response(content, is_bytes=True)
            else:
                page = int(url.split('page-')[-1])
                html = self.html_by_page.get(page, '')
                return mock_response(html)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    return MockSession


@pytest.fixture
def sample_records():
    def _sample_records(date: date = date.today(), rec_count: int = 5):
        return [
            SpimexTradingResultSchema(
                exchange_product_id=f'ID{i}',
                exchange_product_name='Test Product',
                delivery_basis_name='Test Basis',
                volume=100,
                total=1000,
                count=1,
                date=date
            ) for i in range(rec_count)
        ]
    return _sample_records


@pytest.fixture
def semaphore():
    return asyncio.Semaphore(5)


@pytest.fixture(scope='session')
def docker_compose_file():
    return [str(Path(__file__).parent / 'docker-compose.yml')]


@pytest.fixture(scope='session')
def postgres_service(docker_services):
    def is_responsive():
        import psycopg2
        try:
            conn = psycopg2.connect(
                dbname='testdb',
                user='testuser',
                password='testpass',
                host='localhost',
                port=5433,
            )
            conn.close()
            return True
        except psycopg2.OperationalError:
            return False

    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=5.0,
        check=is_responsive
    )
    return {
        'host': '127.0.0.1',
        'port': 5433,
        'user': 'testuser',
        'password': 'testpass',
        'db': 'testdb',
    }


@pytest_asyncio.fixture
async def async_test_session(postgres_service):

    dsn = (
        f'postgresql+asyncpg://{postgres_service["user"]}:'
        f'{postgres_service["password"]}@{postgres_service["host"]}:'
        f'{postgres_service["port"]}/{postgres_service["db"]}'
    )
    engine = create_async_engine(dsn, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield async_session
    await engine.dispose()


@pytest_asyncio.fixture()
async def clean_db(async_test_session):
    async with async_test_session() as session:
        await session.execute(delete(SpimexTradingResult))
        await session.commit()
