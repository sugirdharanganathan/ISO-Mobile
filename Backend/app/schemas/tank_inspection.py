from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TankInspectionCreate(BaseModel):
    """Schema for creating a new tank inspection"""
    tank_number: str
    status_name: str
    inspection_type_name: str
    product_name: str
    location_name: str
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None
    notes: Optional[str] = None
    inspection_date: Optional[datetime] = None
    created_by: Optional[str] = None
    operator_id: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "tank_number": "TK001",
                "status_name": "Laden",
                "inspection_type_name": "Incoming",
                "product_name": "Liquid Argon",
                "location_name": "SG-1 16A, Benoi Cresent",
                "safety_valve_brand": "Brand A",
                "safety_valve_model": "Model X",
                "safety_valve_size": "3",
                "notes": "All checks passed",
                "inspection_date": "2025-11-18T10:00:00",
                "created_by": "admin",
                "operator_id": 1,
            }
        }


class TankInspectionResponse(BaseModel):
    """Schema for tank inspection response"""
    inspection_id: int
    tank_number: str
    report_number: str
    inspection_date: datetime
    status_id: Optional[int] = None
    product_id: Optional[int] = None
    inspection_type_id: Optional[int] = None
    location_id: Optional[int] = None
    working_pressure: Optional[str] = None
    frame_type: Optional[str] = None
    design_temperature: Optional[str] = None
    cabinet_type: Optional[str] = None
    mfgr: Optional[str] = None
    pi_next_inspection_date: Optional[datetime] = None
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None
    notes: Optional[str] = None
    created_by: Optional[str] = None
    operator_id: Optional[int] = None
    operator_name: Optional[str] = None
    ownership: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
