from sqlalchemy import Column, String

from database.models.base import Base


class FX_Rates(Base):
    __tablename__ = 'FX_RATES'
    base_cur = Column(String(3), primary_key=True)
    quote_cur = Column(String(3), primary_key=True)
    duration = Column(String, primary_key=True)
