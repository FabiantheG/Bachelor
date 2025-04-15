from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class FX_TS(Base):
    __tablename__ = "FX_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('FX_REF.series_id'), primary_key=True)
