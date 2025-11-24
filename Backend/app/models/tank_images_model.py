from sqlalchemy import Column, Integer, String, DateTime, Date, Text, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base

class TankImages(Base):
    __tablename__ = "tank_images"

    id = Column(Integer, primary_key=True, autoincrement=True)

    emp_id = Column(Integer, nullable=True)
    inspection_id = Column(Integer, ForeignKey("tank_inspection_details.inspection_id", ondelete="SET NULL"), nullable=True, index=True)

    # image_id stores the id chosen by user and references image_type(id)
    image_id = Column(Integer, ForeignKey("image_type.id", ondelete="SET NULL"), nullable=True, index=True)

    # keep legacy textual column (slug/label) for quick lookups/display
    image_type = Column(String(50), nullable=False)

    tank_number = Column(String(50), nullable=False)
    image_path = Column(String(255), nullable=False)
    thumbnail_path = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    created_date = Column(Date, nullable=True)
