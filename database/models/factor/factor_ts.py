from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class FACTOR_TS(Base):
    __tablename__ = "FACTOR_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('FACTOR_REF.series_id'), primary_key=True)
