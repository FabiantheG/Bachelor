from sqlalchemy import Column, Integer, String

from database.models.base import Base


class  Portfolio(Base):
    __tablename__ = "PORTFOLIO"
    portfolio_id = Column(Integer, primary_key=True)
    name = Column(String)
    investor_cur = Column(String(3))
