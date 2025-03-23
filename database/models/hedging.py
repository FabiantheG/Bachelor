from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from .base import Base

class Hedging_Strategy(Base):
    __tablename__ = "HEDGING_STRATEGY"
    hedge_id = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(Float)




class Simulation_Ref(Base):
    __tablename__ = "SIMULATION_REF"
    simulation_id = Column(Integer, primary_key=True)
    hedge_id = Column(Integer, ForeignKey('HEDGING_STRATEGY.hedge_id'))
    portfolio_id = Column(Integer, ForeignKey('PORTFOLIO.portfolio_id'))


class Simulation_TS(Base):
    __tablename__ = "SIMULATION_TS"
    date = Column(Date, primary_key=True)
    simulation_id = Column(Integer, ForeignKey('SIMULATION_REF.simulation_id'), primary_key=True)
    unhedged_growth =Column(Float)
    hedged_growth = Column(Float)