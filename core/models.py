from datetime import datetime

from sqlalchemy import Date, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Базовый класс для всех моделей SQLAlchemy."""

    pass


class SpimexTradingResult(Base):
    """Модель для хранения результатов торгов на СПИМЭКС."""

    __tablename__ = 'spimex_trading_results'

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange_product_id: Mapped[str] = mapped_column(String, nullable=False)
    exchange_product_name: Mapped[str] = mapped_column(String, nullable=False)
    oil_id: Mapped[str] = mapped_column(String(4), nullable=False)
    delivery_basis_id: Mapped[str] = mapped_column(String(3), nullable=False)
    delivery_basis_name: Mapped[str] = mapped_column(String, nullable=False)
    delivery_type_id: Mapped[str] = mapped_column(String(1), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)
    total: Mapped[int] = mapped_column(Integer, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    created_on: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now().replace(microsecond=0)
    )
    updated_on: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now().replace(microsecond=0)
    )
