from sqlalchemy import Column, Integer, String, Float

from database.models.base import Base


class Hedging_Strategy(Base):
    __tablename__ = "HEDGING_STRATEGY"
    hedge_id = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(Float)
