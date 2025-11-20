from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class InspectionChecklist(Base):
    __tablename__ = "inspection_checklist"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(Integer, ForeignKey("inspection_report.id", ondelete="CASCADE"), nullable=False)
    tank_number = Column(String(50), nullable=True)
    job_id = Column(Integer, nullable=True)
    job_name = Column(String(255), nullable=True)
    sub_job_id = Column(Integer, nullable=True)
    sn = Column(String(16), nullable=False)
    sub_job_description = Column(String(512), nullable=True)
    status_id = Column(Integer, nullable=False, default=1)
    status = Column(String(32), nullable=True)
    comment = Column(Text, nullable=True)
    flagged = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationship back to inspection_report
    report = relationship("InspectionReport", back_populates="checklists")
