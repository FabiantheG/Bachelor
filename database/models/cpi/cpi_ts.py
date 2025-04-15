from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class CPI_TS(Base):
    __tablename__ = "CPI_TS"
    date = Column(Date, primary_key=True, comment='date of the CPI')
    rate = Column(Float, comment='rate of the CPI')
    series_id = Column(Integer, ForeignKey('CPI_REF.series_id'), primary_key=True)





