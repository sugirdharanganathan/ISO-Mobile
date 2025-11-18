from sqlalchemy import Column, Integer, String, TIMESTAMP, Date, func
from app.database import Base

class TankImage(Base):
    __tablename__ = "tank_images"

    id = Column(Integer, primary_key=True, index=True)
    tank_number = Column(String(50), nullable=False, index=True)
    image_type = Column(String(50), nullable=False, index=True)
    image_path = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    created_date = Column(Date)
