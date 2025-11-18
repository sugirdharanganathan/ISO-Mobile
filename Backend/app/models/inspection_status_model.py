from sqlalchemy import Column, Integer, String, DateTime, func
from app.database import Base


class InspectionStatus(Base):
    __tablename__ = "inspection_status"

    status_id = Column(Integer, primary_key=True, autoincrement=True)
    status_name = Column(String(32), nullable=False, unique=True)
    description = Column(String(255), nullable=True)
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
