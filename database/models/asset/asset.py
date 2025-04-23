from sqlalchemy import Column, String

from database.models.base import Base


class ASSET(Base):
    __tablename__ = 'ASSET'
    asset_ticker = Column(String, primary_key=True, comment= 'balababa')
    currency = Column(String)
