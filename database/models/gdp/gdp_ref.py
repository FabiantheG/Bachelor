from sqlalchemy import Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship

from database.models.base import Base


class GDP_REF(Base):
    __tablename__ = "GDP_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'), comment='provider id')
    country = Column(String(30), ForeignKey('GDP_RATES.country'), comment='country code')

    indicator = relationship(
        "ECONOMIC_INDICATOR",
        primaryjoin="GDP_REF.series_id == foreign(ECONOMIC_INDICATOR.series_id)",
        uselist=False
    )
