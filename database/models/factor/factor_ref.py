from sqlalchemy import Column, Integer, ForeignKey, String

from database.models.base import Base


class FACTOR_REF(Base):
    __tablename__ = "FACTOR_REF"
    series_id = Column(Integer, primary_key=True, autoincrement=True)
    factor_id = Column(Integer, ForeignKey('FACTOR.factor_id'))

