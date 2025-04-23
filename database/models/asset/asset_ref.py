from sqlalchemy import Column, Integer, ForeignKey, String

from database.models.base import Base


class ASSET_REF(Base):
    __tablename__ = "ASSET_REF"
    series_id = Column(Integer, primary_key=True, autoincrement=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    asset_ticker = Column(String, ForeignKey('ASSET.asset_ticker'))
