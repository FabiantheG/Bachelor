from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, ForeignKeyConstraint
from .base import Base


class FX_Rates(Base):
    __tablename__ = 'FX_RATES'
    base_cur = Column(String(3), primary_key=True)
    quote_cur = Column(String(3), primary_key=True)
    duration = Column(String, primary_key=True)


class FX_Ref(Base):
    __tablename__ = "FX_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    base_cur = Column(String(3))
    quote_cur = Column(String(3))
    duration = Column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            ['base_cur', 'quote_cur', 'duration'],
            ['FX_RATES.base_cur', 'FX_RATES.quote_cur', 'FX_RATES.duration']
        ),
    )
class FX_TS(Base):
    __tablename__ = "FX_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('FX_REF.series_id'), primary_key=True)
