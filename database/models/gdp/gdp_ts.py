from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class GDP_TS(Base):
    __tablename__ = 'GDP_TS'
    date = Column(Date, primary_key=True)
    rate = Column(Float)
    series_id = Column(Integer, ForeignKey('GDP_REF.series_id'), primary_key=True)
