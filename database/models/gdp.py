from sqlalchemy import Column, Integer, String, Date, Double, ForeignKey
from .base import Base
from sqlalchemy.orm import relationship

class GDP_Rates(Base):
    __tablename__ = 'GDP_RATES'
    currency = Column(String(3), primary_key=True)

class GDP_Ref(Base):
    __tablename__ = "GDP_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'), comment='provider id')
    currency = Column(String(3), ForeignKey('GDP_RATES.currency'), comment='currency code')

    indicator = relationship(
        "Economic_Indicator",
        primaryjoin="GDP_Ref.series_id == Economic_Indicator.series_id",
        uselist=False,
        viewonly=True)


class GDP_TS(Base):
    __tablename__ = 'GDP_TS'
    series_id = Column(Integer, ForeignKey('GDP_REF.series_id'), primary_key=True)
    date = Column(Date, primary_key=True)
    rate = Column(Double)



