from sqlalchemy import Column, Integer, String, DateTime, Text, func
from app.database import Base


class InspectionType(Base):
    __tablename__ = "inspection_type"

    inspection_type_id = Column(Integer, primary_key=True, autoincrement=True)
    inspection_type_name = Column(String(150), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
