from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base


class SIMULATION_TS(Base):
    __tablename__ = "SIMULATION_TS"
    date = Column(Date, primary_key=True)
    simulation_id = Column(Integer, ForeignKey('SIMULATION_REF.simulation_id'), primary_key=True)
    unhedged_growth =Column(Float)
    hedged_growth = Column(Float)