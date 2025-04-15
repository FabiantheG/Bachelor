from sqlalchemy import Column, Integer, String
from .base import Base


class Provider(Base):
    __tablename__ = 'PROVIDER'
    provider_id = Column(Integer, primary_key=True)
    name = Column(String)