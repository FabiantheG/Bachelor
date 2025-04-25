from sqlalchemy import Column, String, Integer, Float

from database.models.base import Base


class FACTOR(Base):
    __tablename__ = 'FACTOR'
    factor_id = Column(Integer, primary_key=True)
    name_factor = Column(String)
    version = Column(Float)
