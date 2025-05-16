import os

from dotenv import load_dotenv

load_dotenv()

DB_NAME: str = os.getenv('POSTGRES_DB')
DB_HOST: str = os.getenv('DB_HOST')
DB_PORT: str = os.getenv('DB_PORT')
DB_USER: str = os.getenv('POSTGRES_USER')
DB_PASS: str = os.getenv('POSTGRES_PASSWORD')

DATABASE_URL: str = (
    f'postgresql+asyncpg://{DB_USER}:{DB_PASS}@'
    f'{DB_HOST}:{DB_PORT}/{DB_NAME}'
)
ALEMBIC_SYNC_DB_URL: str = (
    f'postgresql://{DB_USER}:{DB_PASS}@'
    f'{DB_HOST}:{DB_PORT}/{DB_NAME}'
)
