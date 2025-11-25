from sqlalchemy import Column, Integer, String, DateTime, Text
from datetime import datetime

# ✅ use the shared Base from database.py
from app.database import Base


class ToDoList(Base):
    """Model for to_do_list table - stores flagged inspection checklist items"""
    __tablename__ = "to_do_list"

    id = Column(Integer, primary_key=True, index=True)
    checklist_id = Column(Integer, nullable=False, index=True)  # FK to inspection_checklist.id
    inspection_id = Column(Integer, nullable=False, index=True)  # FK to tank_inspection_details.inspection_id
    tank_id = Column(Integer, nullable=True, index=True)
    job_name = Column(String(255), nullable=True)
    sub_job_description = Column(String(512), nullable=True)
    sn = Column(String(16), nullable=False)
    status_id = Column(Integer, nullable=True)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def as_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            "id": self.id,
            "checklist_id": self.checklist_id,
            "inspection_id": self.inspection_id,
            "tank_id": self.tank_id,
            "job_name": self.job_name,
            "sub_job_description": self.sub_job_description,
            "sn": self.sn,
            "status_id": self.status_id,
            "comment": self.comment,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
