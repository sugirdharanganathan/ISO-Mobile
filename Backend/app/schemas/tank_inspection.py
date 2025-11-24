from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# ---------------------------------------------------------
# CREATE SCHEMA — user enters tank_id instead of tank_number
# ---------------------------------------------------------
class TankInspectionCreate(BaseModel):
    created_by: str = Field(..., description="User who created the record")
    tank_id: int = Field(..., description="Foreign key: tank_details.tank_id")
    status_id: int
    product_id: int
    inspection_type_id: int
    location_id: int
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None
    safety_valve_size_id: Optional[int] = None
    notes: Optional[str] = None
    operator_id: Optional[int] = None

    class Config:
        schema_extra = {
            "example": {
                "created_by": "user@example.com",
                "tank_id": 12,
                "status_id": 1,
                "product_id": 3,
                "inspection_type_id": 2,
                "location_id": 4,
                "safety_valve_brand_id": 2,
                "safety_valve_model_id": 1,
                "safety_valve_size_id": 1,
                "notes": "All checks OK",
                "operator_id": 55
            }
        }


# ---------------------------------------------------------
# RESPONSE SCHEMA — tank_number present because DB stores it
# ---------------------------------------------------------
class TankInspectionResponse(BaseModel):
    inspection_id: int

    status_id: Optional[int] = None
    product_id: Optional[int] = None
    inspection_type_id: Optional[int] = None
    location_id: Optional[int] = None

    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None
    safety_valve_size_id: Optional[int] = None

    class Config:
        orm_mode = True


# ---------------------------------------------------------
# UPDATE SCHEMA — tank_id/tank_number removed fully
# ---------------------------------------------------------
class TankInspectionUpdate(BaseModel):
    inspection_date: Optional[datetime] = None

    # master fields
    status_id: Optional[int] = None
    status_name: Optional[str] = None

    inspection_type_id: Optional[int] = None
    inspection_type_name: Optional[str] = None

    product_id: Optional[int] = None
    product_name: Optional[str] = None

    location_id: Optional[int] = None
    location_name: Optional[str] = None

    # tank details
    working_pressure: Optional[float] = None
    frame_type: Optional[str] = None
    design_temperature: Optional[float] = None
    cabinet_type: Optional[str] = None
    mfgr: Optional[str] = None

    # safety valve
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None

    # misc
    notes: Optional[str] = None
    operator_id: Optional[int] = None
    ownership: Optional[str] = None

    class Config:
        from_attributes = True
