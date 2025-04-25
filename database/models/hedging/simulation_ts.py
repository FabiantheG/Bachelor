from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base



class SIMULATION_TS(Base):
    __tablename__ = "SIMULATION_TS"
    simulation_id = Column(Integer, ForeignKey('SIMULATION_REF.simulation_id'), primary_key=True, nullable=False)
    date = Column(Date, primary_key=True, nullable=False)
    unhedged_growth = Column(Float, nullable=False)
    hedged_growth = Column(Float, nullable=False)








