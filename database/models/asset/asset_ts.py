from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class ASSET_TS(Base):
    __tablename__ = "ASSET_TS"
    date = Column(Date, primary_key=True)
    close = Column(Float)
    series_id = Column(Integer, ForeignKey('ASSET_REF.series_id'), primary_key=True)
