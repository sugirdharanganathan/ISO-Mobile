"""
Pydantic schemas for tank inspection endpoints.
Handles request validation and response serialization.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import date, datetime
from decimal import Decimal


class TankInspectionCreate(BaseModel):
    """
    Request schema for creating a tank inspection record.
    Expects only client-provided inputs; auto-filled fields are computed server-side.
    """
    
    # Required fields (must be provided by client)
    tank_number: str = Field(..., description="Tank identifier (must exist in tank_header)")
    status_name: str = Field(..., description="Status name from tank_status table")
    inspection_type_name: str = Field(..., description="Inspection type from inspection_type table")
    product_name: str = Field(..., description="Product name from product_master table")
    location_name: str = Field(..., description="Location name from location_master table")
    
    # Optional fields
    inspection_date: Optional[datetime] = Field(None, description="Inspection date; defaults to today if not provided")
    notes: Optional[str] = Field(None, description="Optional inspection notes or observations")
    created_by: Optional[str] = Field(None, description="User who created the inspection record")
    operator_id: Optional[int] = Field(None, description="Employee ID (emp_id) of the operator; operator_name will be auto-filled")
    
    @validator('tank_number')
    def tank_number_not_empty(cls, v):
        """Ensure tank_number is not empty."""
        if not v or not v.strip():
            raise ValueError('tank_number must not be empty')
        return v.strip()
    
    @validator('status_name')
    def status_name_not_empty(cls, v):
        """Ensure status_name is not empty."""
        if not v or not v.strip():
            raise ValueError('status_name must not be empty')
        return v.strip()
    
    @validator('inspection_type_name')
    def inspection_type_name_not_empty(cls, v):
        """Ensure inspection_type_name is not empty."""
        if not v or not v.strip():
            raise ValueError('inspection_type_name must not be empty')
        return v.strip()
    
    @validator('product_name')
    def product_name_not_empty(cls, v):
        """Ensure product_name is not empty."""
        if not v or not v.strip():
            raise ValueError('product_name must not be empty')
        return v.strip()
    
    @validator('location_name')
    def location_name_not_empty(cls, v):
        """Ensure location_name is not empty."""
        if not v or not v.strip():
            raise ValueError('location_name must not be empty')
        return v.strip()

    # Allow client to provide safety valve info independently
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None
    
    class Config:
        schema_extra = {
            "example": {
                "tank_number": "TANK-001",
                "status_name": "OK",
                "inspection_type_name": "Incoming",
                "product_name": "Liquid Argon",
                "location_name": "SG-1 16A, Benoi Cresent",
                "inspection_date": "2025-11-17T10:00:00",
                "notes": "All checks passed",
                "created_by": "user@example.com",
                "operator_id": 1001
            }
        }


class TankInspectionResponse(BaseModel):
    """
    Response schema for a created tank inspection record.
    Returns all fields including auto-filled values and generated report number.
    """
    
    inspection_id: int
    inspection_date: datetime
    report_number: str
    tank_number: str
    
    # Master table IDs (resolved from names)
    status_id: int
    product_id: int
    inspection_type_id: int
    location_id: int
    
    # Auto-filled from tank_details
    working_pressure: Optional[Decimal] = None
    frame_type: Optional[str] = None
    design_temperature: Optional[str] = None
    cabinet_type: Optional[str] = None
    mfgr: Optional[str] = None
    pi_next_inspection_date: Optional[date] = None
    
    # Auto-filled from tank_mobile
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None
    
    # Optional fields
    notes: Optional[str] = None
    
    # Operator fields
    operator_id: Optional[int] = None
    operator_name: Optional[str] = None
    # Safety valve fields (provided by client; independent from tank_mobile)
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None
    # Ownership derived from tank_details.lease (OWN or LEASED)
    ownership: Optional[str] = None
    
    # Audit fields
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    class Config:
        from_attributes = True  # Allow ORM mode for SQLAlchemy models
        schema_extra = {
            "example": {
                "inspection_id": 1,
                "inspection_date": "2025-11-17T10:00:00",
                "report_number": "SG-T1-17112025-01",
                "tank_number": "TANK-001",
                "status_id": 1,
                "product_id": 1,
                "inspection_type_id": 1,
                "location_id": 1,
                "working_pressure": "10.00",
                "frame_type": "Frame Type A",
                "design_temperature": "25°C",
                "cabinet_type": "Standard",
                "mfgr": "Manufacturer XYZ",
                "pi_next_inspection_date": "2025-12-01",
                "safety_valve_brand": "Brand A",
                "safety_valve_model": "Model X",
                "safety_valve_size": "10mm",
                "notes": "All checks passed",
                "operator_id": 1001,
                "operator_name": "John Doe",
                "safety_valve_brand": "Crosby",
                "safety_valve_model": "SR / 1SR",
                "safety_valve_size": "½\"–4\" (DN15–DN100)",
                "ownership": "OWN",
                "created_at": "2025-11-17T10:00:00",
                "updated_at": "2025-11-17T10:00:00",
                "created_by": "user@example.com"
            }
        }


class TankInspectionListResponse(BaseModel):
    """Response wrapper for list of inspections."""
    success: bool
    data: list[TankInspectionResponse]
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "data": [
                    {
                        "inspection_id": 1,
                        "inspection_date": "2025-11-17T10:00:00",
                        "report_number": "SG-T1-17112025-01",
                        "tank_number": "TANK-001",
                        # ... other fields ...
                    }
                ]
            }
        }
