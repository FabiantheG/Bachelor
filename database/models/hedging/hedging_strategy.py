from sqlalchemy import Column, Integer, String, Float, UniqueConstraint
from database.models.base import Base



class HEDGING_STRATEGY(Base):
    __tablename__ = "HEDGING_STRATEGY"
    hedge_id = Column(Integer, primary_key=True)  # Eindeutiger Primärschlüssel
    hedge_name = Column(String, nullable=False)
    version = Column(Float, nullable=False)

    # Eindeutigkeit für hedge_name und version
    __table_args__ = (UniqueConstraint('hedge_name', 'version', name='uq_hedge_name_version'),)