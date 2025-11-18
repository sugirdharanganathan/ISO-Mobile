from sqlalchemy import Column, Integer, String, DateTime, Text, func
from app.database import Base


class SafetyValveBrand(Base):
    __tablename__ = "safety_valve_brand"

    id = Column(Integer, primary_key=True, autoincrement=True)
    brand_name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
