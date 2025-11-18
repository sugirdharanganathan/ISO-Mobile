"""
SQLAlchemy model for tank_inspection_details table.
Stores inspection records with denormalized fields from tank_details and tank_mobile
for performance and audit traceability.
"""

from sqlalchemy import (
    Column, Integer, String, Numeric, Text, DateTime, Date,
    func, Index
)
from sqlalchemy.orm import relationship
from datetime import datetime

# Assuming Base is imported from a central location (e.g., app.database)
# Replace 'Base' with your actual declarative base if using different import
try:
    from app.database import Base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base
    Base = declarative_base()


class TankInspectionDetails(Base):
    """
    Tank Inspection Details model.
    Stores inspection snapshots with auto-filled denormalized fields from related tables.
    """
    __tablename__ = "tank_inspection_details"

    # Primary key
    inspection_id = Column(Integer, primary_key=True, index=True)

    # Timestamps
    inspection_date = Column(DateTime, nullable=False, default=func.now())
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    # Report identifier (unique per record)
    report_number = Column(String(50), nullable=False, unique=True, index=True)

    # Foreign keys to master tables
    tank_number = Column(String(50), nullable=False, index=True)
    status_id = Column(Integer, nullable=False, index=True)
    product_id = Column(Integer, nullable=False, index=True)
    inspection_type_id = Column(Integer, nullable=False)
    location_id = Column(Integer, nullable=False)

    # Auto-filled from tank_details (denormalized)
    working_pressure = Column(Numeric(12, 2))
    frame_type = Column(String(255))
    design_temperature = Column(String(100))
    cabinet_type = Column(String(255))
    mfgr = Column(String(255))  # Tank Manufacturer

    # Auto-filled from tank_mobile (denormalized)
    safety_valve_brand = Column(String(255))
    safety_valve_model = Column(String(255))
    safety_valve_size = Column(String(100))

    # PI certificate info
    pi_next_inspection_date = Column(Date)

    # Optional inspection notes
    notes = Column(Text)

    # Operator fields (auto-filled when operator_id is set)
    operator_id = Column(Integer, nullable=True, index=True)  # FK to users.emp_id
    operator_name = Column(String(100), nullable=True)  # Auto-filled from users.name
    # Ownership: OWN or LEASED (derived from tank_details.lease)
    ownership = Column(String(16), nullable=True, index=True)

    # Audit fields
    created_by = Column(String(100))
    updated_by = Column(String(100))

    def __repr__(self):
        return (
            f"<TankInspectionDetails(inspection_id={self.inspection_id}, "
            f"report_number='{self.report_number}', "
            f"tank_number='{self.tank_number}', "
            f"inspection_date={self.inspection_date})>"
        )

    @property
    def as_dict(self):
        """Convert model instance to dictionary for JSON serialization."""
        return {
            "inspection_id": self.inspection_id,
            "inspection_date": self.inspection_date.isoformat() if self.inspection_date else None,
            "report_number": self.report_number,
            "tank_number": self.tank_number,
            "status_id": self.status_id,
            "product_id": self.product_id,
            "inspection_type_id": self.inspection_type_id,
            "location_id": self.location_id,
            "working_pressure": float(self.working_pressure) if self.working_pressure else None,
            "frame_type": self.frame_type,
            "design_temperature": self.design_temperature,
            "cabinet_type": self.cabinet_type,
            "mfgr": self.mfgr,
            "safety_valve_brand": self.safety_valve_brand,
            "safety_valve_model": self.safety_valve_model,
            "safety_valve_size": self.safety_valve_size,
            "pi_next_inspection_date": (
                self.pi_next_inspection_date.isoformat() if self.pi_next_inspection_date else None
            ),
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "operator_id": self.operator_id,
            "operator_name": self.operator_name,
            "ownership": self.ownership,
        }


# Optional: Define indexes at the module level if ORM doesn't support them all at the table level
__table_args__ = (
    Index('idx_tank_inspection_tank_number', 'tank_number'),
    Index('idx_tank_inspection_report_number', 'report_number'),
    Index('idx_tank_inspection_inspection_date', 'inspection_date'),
    Index('idx_tank_inspection_operator_id', 'operator_id'),
    Index('idx_tank_inspection_ownership', 'ownership'),
)
