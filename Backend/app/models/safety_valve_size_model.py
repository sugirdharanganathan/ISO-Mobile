from sqlalchemy import Column, Integer, String, DateTime, Text, func
from app.database import Base


class SafetyValveSize(Base):
    __tablename__ = "safety_valve_size"

    id = Column(Integer, primary_key=True, autoincrement=True)
    size_label = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
