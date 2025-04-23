from sqlalchemy import Column, Integer, ForeignKey

from database.models.base import Base


class SIMULATION_REF(Base):
    __tablename__ = "SIMULATION_REF"
    simulation_id = Column(Integer, primary_key=True)
    hedge_id = Column(Integer, ForeignKey('HEDGING_STRATEGY.hedge_id'))
    portfolio_id = Column(Integer, ForeignKey('PORTFOLIO.portfolio_id'))
