from sqlalchemy import Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship

from database.models.base import Base


class CPI_REF(Base):
    __tablename__ = "CPI_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'), comment='provider id')
    currency = Column(String(3), ForeignKey('CPI_RATES.currency'), comment='currency code')

    indicator = relationship(
        "ECONOMIC_INDICATOR",
        primaryjoin="CPI_REF.series_id == foreign(ECONOMIC_INDICATOR.series_id)",
        uselist=False
    )
