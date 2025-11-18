from sqlalchemy import Column, Integer, String, DateTime, Date, Text, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class TankImages(Base):
    __tablename__ = "tank_images"

    id = Column(Integer, primary_key=True, autoincrement=True)
    emp_id = Column(Integer, nullable=True)
    tank_number = Column(String(50), nullable=False)
    image_type = Column(String(50), nullable=False)
    image_path = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_date = Column(Date, nullable=True)
