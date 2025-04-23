from sqlalchemy import Column, Integer, ForeignKey, String, ForeignKeyConstraint

from database.models.base import Base


class FX_REF(Base):
    __tablename__ = "FX_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    base_cur = Column(String(3))
    quote_cur = Column(String(3))
    duration = Column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            ['base_cur', 'quote_cur', 'duration'],
            ['FX_RATES.base_cur', 'FX_RATES.quote_cur', 'FX_RATES.duration']
        ),
    )
