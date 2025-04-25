from sqlalchemy import Column, Integer, String

from database.models.base import Base


class  PORTFOLIO(Base):
    __tablename__ = "PORTFOLIO"
    portfolio_id = Column(Integer, primary_key=True)
    portfolio_name = Column(String)
    investor_cur = Column(String(3))
