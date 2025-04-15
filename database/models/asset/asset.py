from sqlalchemy import Column, String

from database.models.base import Base


class Asset(Base):
    __tablename__ = 'ASSET'
    asset_ticker = Column(String, primary_key=True, comment= 'balababa')
    currency = Column(String)
