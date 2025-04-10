from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from .base import Base

class Interest_Rate(Base):
    __tablename__ = 'INTEREST_RATE'
    ir_id = Column(Integer, primary_key=True)
    currency = Column(String(3))
    duration = Column(String(5))


class IR_Ref(Base):
    __tablename__ = "IR_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    ir_id = Column(Integer, ForeignKey('INTEREST_RATE.ir_id'))


class IR_TS(Base):
    __tablename__ = "IR_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('IR_REF.series_id'), primary_key=True)
