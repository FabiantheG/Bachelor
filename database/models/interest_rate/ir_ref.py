from sqlalchemy import Column, Integer, ForeignKey

from database.models.base import Base


class IR_Ref(Base):
    __tablename__ = "IR_REF"
    series_id = Column(Integer, primary_key=True)
    provider_id = Column(Integer, ForeignKey('PROVIDER.provider_id'))
    ir_id = Column(Integer, ForeignKey('INTEREST_RATE.ir_id'))
