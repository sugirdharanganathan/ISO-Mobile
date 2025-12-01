from sqlalchemy import Column, Integer, String
from app.database import Base

class ChecklistMaster(Base):
    __tablename__ = "checklist_master"

    # This matches your screenshot exactly
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, nullable=False)
    sub_job_id = Column(Integer, nullable=False)
    sn = Column(String(16), nullable=False)
    sub_job_name = Column(String(255), nullable=False)
    sub_job_description = Column(String(255), nullable=True)