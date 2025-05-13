from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.config import DATABASE_URL
from core.models import Base

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


def init_db() -> None:
    """Создает все таблицы в базе данных, если они еще не существуют."""

    import core.models as models  # noqa: F401

    Base.metadata.create_all(bind=engine)
