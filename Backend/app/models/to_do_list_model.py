from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class ToDoList(Base):
    """Model for to_do_list table - stores flagged inspection checklist items"""
    __tablename__ = "to_do_list"

    id = Column(Integer, primary_key=True, index=True)
    checklist_id = Column(Integer, nullable=False, index=True)  # FK to inspection_checklist.id
    report_id = Column(Integer, nullable=False)
    tank_number = Column(String(50), nullable=False)
    job_name = Column(String(255), nullable=True)
    sub_job_description = Column(String(512), nullable=True)
    sn = Column(String(16), nullable=False)
    status = Column(String(32), nullable=True)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def as_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            "id": self.id,
            "checklist_id": self.checklist_id,
            "report_id": self.report_id,
            "tank_number": self.tank_number,
            "job_name": self.job_name,
            "sub_job_description": self.sub_job_description,
            "sn": self.sn,
            "status": self.status,
            "comment": self.comment,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
