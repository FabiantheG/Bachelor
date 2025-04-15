from sqlalchemy import Column, Integer, String, ForeignKey
from .base import Base


class Economic_Indicator(Base):
    __tablename__ = "ECONOMIC_INDICATOR"
    series_id = Column(Integer, primary_key=True)
    indicator_type = Column(String, nullable=False)
