from sqlalchemy import Column, Integer, String, DateTime, Text, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base


class InspectionJob(Base):
    __tablename__ = "inspection_job"

    # Map Python attr 'job_id' to DB column 'id' (some DB schemas use 'id' as PK)
    job_id = Column('id', Integer, primary_key=True, autoincrement=True)
    job_code = Column(String(32), nullable=True)
    job_description = Column(String(255), nullable=False)
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationship to inspection_sub_job
    sub_jobs = relationship("InspectionSubJob", back_populates="job", cascade="all, delete-orphan")
