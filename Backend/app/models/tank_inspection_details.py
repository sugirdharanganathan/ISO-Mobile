# app/models/tank_inspection_details.py
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, Numeric, Text, DateTime, Date, func, Index, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from app.database import Base

class TankInspectionDetails(Base):
    __tablename__ = "tank_inspection_details"

    inspection_id = Column(Integer, primary_key=True, index=True)
    inspection_date = Column(DateTime, nullable=False, default=func.now())
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    report_number = Column(String(50), nullable=False, unique=True, index=True)
    tank_id = Column(Integer, ForeignKey("tank_details.tank_id", ondelete="SET NULL"), nullable=True, index=True)
    tank_number = Column(String(50), nullable=False, index=True)
    status_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False, index=True)
    inspection_type_id = Column(Integer, nullable=False)
    location_id = Column(Integer, nullable=False)
    working_pressure = Column(Numeric(12, 2))
    design_temperature = Column(Numeric(12, 2))
    frame_type = Column(String(255))
    cabinet_type = Column(String(255))
    mfgr = Column(String(255))
    safety_valve_brand_id = Column(Integer, nullable=True, index=True)
    safety_valve_model_id = Column(Integer, nullable=True, index=True)
    safety_valve_size_id = Column(Integer, nullable=True, index=True)
    pi_next_inspection_date = Column(Date)
    notes = Column(Text)
    lifter_weight = Column(String(255), nullable=True)
    emp_id = Column(Integer, ForeignKey("users.emp_id"), nullable=True, index=True)
    operator_id = Column(Integer, nullable=True, index=True)
    ownership = Column(String(16), nullable=True, index=True)
    created_by = Column(String(100))
    updated_by = Column(String(100))

    # Relationship back to inspection_checklist items
    checklists = relationship("InspectionChecklist", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<TankInspectionDetails(inspection_id={self.inspection_id}, "
            f"report_number='{self.report_number}', tank_number='{self.tank_number}')>"
        )

    @property
    def as_dict(self):
        return {
            "inspection_id": self.inspection_id,
            "inspection_date": self.inspection_date.isoformat() if self.inspection_date else None,
            "report_number": self.report_number,
            "tank_number": self.tank_number,
            "status_id": self.status_id,
            "product_id": self.product_id,
            "inspection_type_id": self.inspection_type_id,
            "location_id": self.location_id,
            "working_pressure": float(self.working_pressure) if self.working_pressure is not None else None,
            "frame_type": self.frame_type,
            "design_temperature": float(self.design_temperature) if self.design_temperature is not None else None,
            "cabinet_type": self.cabinet_type,
            "mfgr": self.mfgr,
            "safety_valve_brand_id": self.safety_valve_brand_id,
            "safety_valve_model_id": self.safety_valve_model_id,
            "safety_valve_size_id": self.safety_valve_size_id,
            "pi_next_inspection_date": self.pi_next_inspection_date.isoformat() if self.pi_next_inspection_date else None,
            "notes": self.notes,
            "lifter_weight": self.lifter_weight,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "operator_id": self.operator_id,
            "operator_name": self.operator_name,
            "ownership": self.ownership,
        }

__table_args__ = (
    Index('idx_tank_inspection_tank_number', 'tank_number'),
    Index('idx_tank_inspection_report_number', 'report_number'),
    Index('idx_tank_inspection_inspection_date', 'inspection_date'),
    Index('idx_tank_inspection_operator_id', 'operator_id'),
    Index('idx_tank_inspection_ownership', 'ownership'),
)
