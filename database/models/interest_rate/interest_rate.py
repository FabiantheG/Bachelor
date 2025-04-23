from sqlalchemy import Column, Integer, String

from database.models.base import Base


class INTEREST_RATE(Base):
    __tablename__ = 'INTEREST_RATE'
    ir_id = Column(Integer, primary_key=True)
    currency = Column(String(3))
    duration = Column(String(5))
