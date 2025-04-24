from sqlalchemy import Column, Integer, Float, Date, ForeignKey
from database.models.base import Base

class SIMULATION_TS(Base):
    __tablename__ = "SIMULATION_TS"
    ts_id           = Column(Integer, primary_key=True, autoincrement=True)
    simulation_id   = Column(Integer, ForeignKey('SIMULATION_REF.simulation_id'), nullable=False)
    date            = Column(Date,   nullable=False)
    unhedged_growth = Column(Float,  nullable=False)
    hedged_growth   = Column(Float,  nullable=False)
