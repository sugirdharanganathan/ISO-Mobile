from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class InspectionSubJob(Base):
    __tablename__ = "inspection_sub_job"

    sub_job_id = Column(Integer, primary_key=True, autoincrement=True)
    # job_id references inspection_job.id in DB; map to Python attr job_id
    job_id = Column(Integer, ForeignKey("inspection_job.id", ondelete="CASCADE"), nullable=False)
    sn = Column(String(32), nullable=True)
    # Keep sub_job_name as the main textual title of the sub-job
    sub_job_name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationship back to inspection_job
    job = relationship("InspectionJob", back_populates="sub_jobs")
