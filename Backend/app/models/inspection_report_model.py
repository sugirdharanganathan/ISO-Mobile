from sqlalchemy import Column, Integer, String, DateTime, Text, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class InspectionReport(Base):
    __tablename__ = "inspection_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tank_number = Column(String(50), nullable=False)
    inspection_date = Column(String(10), nullable=False)  # DATE type
    emp_id = Column(Integer, nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationship to inspection_checklist
    checklists = relationship("InspectionChecklist", back_populates="report", cascade="all, delete-orphan")
