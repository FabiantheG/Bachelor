from sqlalchemy import Column, Integer, String, Float, ForeignKey
from database.models.base import Base


class PORTFOLIO_ASSET_CONNECTION(Base):
    __tablename__ = "PORTFOLIO_ASSET_CONNECTION"
    asset_ticker = Column(String, ForeignKey('ASSET.asset_ticker'),primary_key=True)
    portfolio_id = Column(Integer, ForeignKey('PORTFOLIO.portfolio_id'),primary_key=True)
    weight = Column(Float)