from sqlalchemy import Column, String

from database.models.base import Base


class GDP_Rates(Base):
    __tablename__ = 'GDP_RATES'
    country = Column(String(30), primary_key=True)
