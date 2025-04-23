from sqlalchemy import Column, Integer, String, ForeignKey
from .base import Base


class ECONOMIC_INDICATOR(Base):
    __tablename__ = "ECONOMIC_INDICATOR"
    series_id = Column(Integer, primary_key=False)
    indicator_id = Column(Integer, primary_key=True)
    indicator_type = Column(String, nullable=False)
