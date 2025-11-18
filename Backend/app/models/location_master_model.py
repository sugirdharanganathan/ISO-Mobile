from sqlalchemy import Column, Integer, String, DateTime, Text, func
from app.database import Base


class LocationMaster(Base):
    __tablename__ = "location_master"

    location_id = Column(Integer, primary_key=True, autoincrement=True)
    location_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
