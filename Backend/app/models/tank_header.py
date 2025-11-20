from sqlalchemy import Column, Integer, String, TIMESTAMP, func, ForeignKey
from app.database import Base

class Tank(Base):
    __tablename__ = "tank_header"

    id = Column(Integer, primary_key=True, index=True)
    # New column: tank_id as foreign key to tank_details(tank_id)
    tank_id = Column(Integer, ForeignKey("tank_details.tank_id"), nullable=True)
    tank_number = Column(String(50), nullable=False)
    status = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    created_by = Column(String(100))
    updated_by = Column(String(100))
