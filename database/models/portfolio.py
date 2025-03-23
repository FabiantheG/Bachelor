from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from .base import Base

class  Portfolio(Base):
    __tablename__ = "PORTFOLIO"
    portfolio_id = Column(Integer, primary_key=True)
    name = Column(String)
    investor_cur = Column(String(3))

class portfolio_asset_connection(Base):
    __tablename__ = "PORTFOLIO_ASSET_CONNECTION"
    asset_ticker = Column(String, ForeignKey('ASSET.asset_ticker'),primary_key=True)
    portfolio_id = Column(Integer, ForeignKey('PORTFOLIO.portfolio_id'),primary_key=True)
    weight = Column(Float)