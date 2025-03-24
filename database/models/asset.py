from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from .base import Base

class Asset(Base):
    __tablename__ = 'ASSET'
    asset_ticker = Column(String, primary_key=True, comment= 'balababa')
    currency = Column(String)

class Asset_Ref(Base):
    __tablename__ = "ASSET_REF"
    series_id = Column(Integer, primary_key=True, autoincrement=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    asset_ticker = Column(String, ForeignKey('ASSET.asset_ticker'))

class Asset_TS(Base):
    __tablename__ = "ASSET_TS"
    date = Column(Date, primary_key=True)
    close = Column(Float)
    series_id = Column(Integer, ForeignKey('ASSET_REF.series_id'), primary_key=True)
