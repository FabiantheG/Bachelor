from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import relationship, foreign
from .base import Base

class CPI_Rates(Base):
    __tablename__ = 'CPI_RATES'
    currency = Column(String(3), primary_key=True, comment='currency code')

class CPI_Ref(Base):
    __tablename__ = "CPI_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'), comment='provider id')
    currency = Column(String(3), ForeignKey('CPI_RATES.currency'), comment='currency code')

    indicator = relationship(
        "Economic_Indicator",
        primaryjoin="CPI_Ref.series_id == foreign(Economic_Indicator.series_id)",
        uselist=False
    )

class CPI_TS(Base):
    __tablename__ = "CPI_TS"
    date = Column(Date, primary_key=True, comment='date of the CPI')
    rate = Column(Float, comment='rate of the CPI')
    series_id = Column(Integer, ForeignKey('CPI_REF.series_id'), primary_key=True)





