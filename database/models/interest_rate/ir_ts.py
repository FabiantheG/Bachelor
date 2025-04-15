from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class IR_TS(Base):
    __tablename__ = "IR_TS"
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('IR_REF.series_id'), primary_key=True)
