from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class TankInspectionCreate(BaseModel):
    created_by: str
    inspection_type_name: str
    location_name: str
    notes: Optional[str] = None
    operator_id: Optional[int] = None
    product_name: str
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None
    status_name: str
    tank_number: str


    class Config:
        json_schema_extra = {
            "example": {
                "tank_number": "",
                "status_name": "",
                "inspection_type_name": "",
                "product_name": "",
                "location_name": "",
                "safety_valve_brand": "",
                "safety_valve_model": "",
                "safety_valve_size": "",
                "notes": "",
                "created_by": "",
                "operator_id": None
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
    working_pressure: Optional[float] = None
    design_temperature: Optional[float] = None
    frame_type: Optional[str] = None
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

class TankInspectionUpdate(BaseModel):
    """Request schema for updating a tank inspection record (supports partial updates)."""

    # core / header fields
    inspection_date: Optional[datetime] = None
    tank_number: Optional[str] = None
    # you can send either *_id or *_name (name will be mapped to id)
    status_id: Optional[int] = None
    status_name: Optional[str] = None

    inspection_type_id: Optional[int] = None
    inspection_type_name: Optional[str] = None

    product_id: Optional[int] = None
    product_name: Optional[str] = None

    location_id: Optional[int] = None
    location_name: Optional[str] = None

    # tank_details-ish fields (allow override if needed)
    working_pressure: Optional[float] = None
    frame_type: Optional[str] = None
    design_temperature: Optional[float] = None
    cabinet_type: Optional[str] = None
    mfgr: Optional[str] = None

    # safety valve fields
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None

    # misc
    notes: Optional[str] = None
    operator_id: Optional[int] = None
    ownership: Optional[str] = None  # "OWN" / "LEASED" if you ever want to override

    class Config:
        from_attributes = True
