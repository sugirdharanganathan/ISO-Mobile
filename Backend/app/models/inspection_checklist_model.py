from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, func, ForeignKey, Index
from sqlalchemy.orm import relationship
from app.database import Base

class InspectionChecklist(Base):
    __tablename__ = "inspection_checklist"

    # 1. REMOVE the standard 'id' column
    # id = Column(Integer, primary_key=True, autoincrement=True)

    # 2. Add 'primary_key=True' to inspection_id
    inspection_id = Column(Integer, ForeignKey("tank_inspection_details.inspection_id", ondelete="CASCADE"), primary_key=True, nullable=False, index=True)

    # 3. Add 'primary_key=True' to sub_job_id
    # CRITICAL: This MUST be nullable=False for a Primary Key
    sub_job_id = Column(Integer, primary_key=True, nullable=False)

    # tank_id references tank_details.tank_id (optional, no FK to avoid issues)
    tank_id = Column(Integer, nullable=True, index=True)

    # emp_id (authenticated user's emp id from users.emp_id - optional)
    emp_id = Column(Integer, nullable=True, index=True)

    job_id = Column(Integer, nullable=True)
    job_name = Column(String(255), nullable=True)
    
    sn = Column(String(16), nullable=False)
    sub_job_description = Column(String(512), nullable=True)

    # enforce status_id usage (integer FK to inspection_status.status_id)
    status_id = Column(Integer, nullable=False, default=1)
    # keep optional status string for legacy reads (not used for writes)
    status = Column(String(32), nullable=True)

    comment = Column(Text, nullable=True)
    flagged = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())