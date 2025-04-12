from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import relationship, foreign
from .base import Base

class GDP_Rates(Base):
    __tablename__ = 'GDP_RATES'
    country = Column(String(30), primary_key=True)

class GDP_Ref(Base):
    __tablename__ = "GDP_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'), comment='provider id')
    country = Column(String(30), ForeignKey('GDP_RATES.country'), comment='country code')

    indicator = relationship(
        "Economic_Indicator",
        primaryjoin="GDP_Ref.series_id == foreign(Economic_Indicator.series_id)",
        uselist=False
    )

class GDP_TS(Base):
    __tablename__ = 'GDP_TS'
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('GDP_REF.series_id'), primary_key=True)
