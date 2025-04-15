from sqlalchemy import Column, String

from database.models.base import Base


class CPI_Rates(Base):
    __tablename__ = 'CPI_RATES'
    currency = Column(String(3), primary_key=True, comment='currency code')
