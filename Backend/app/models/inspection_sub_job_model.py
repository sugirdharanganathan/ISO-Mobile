from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class InspectionSubJob(Base):
    __tablename__ = "inspection_sub_job"

    sub_job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("inspection_job.job_id", ondelete="CASCADE"), nullable=False)
    sn = Column(String(16), nullable=False, unique=True)
    sub_job_description = Column(String(512), nullable=False)
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationship back to inspection_job
    job = relationship("InspectionJob", back_populates="sub_jobs")
