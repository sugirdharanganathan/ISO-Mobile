
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from datetime import datetime
from decimal import Decimal
import logging

from app.database import get_db_connection, get_db
from app.models.tank_inspection_details import TankInspectionDetails
from app.models.user_model import User
from app.schemas.tank_inspection import (
    TankInspectionCreate,
    TankInspectionResponse,
)
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tank_inspection_checklist", tags=["tank_inspection"])

class GenericResponse(BaseModel):
    success: bool
    data: List[dict]


def rows_to_list(rows):
    return [r for r in rows]


def fetch_pi_next_inspection_date(db: Session, tank_number: str):
    """Fetch next PI inspection date from tank_certificate for a tank."""
    try:
        row = db.execute(
            text(
                """
                SELECT next_insp_date
                FROM tank_certificate
                WHERE tank_number = :tank_number
                ORDER BY next_insp_date DESC
                LIMIT 1
                """
            ),
            {"tank_number": tank_number},
        ).fetchone()
        return row[0] if row else None
    except Exception as exc:
        logger.warning("Could not fetch PI next inspection date for %s: %s", tank_number, exc)
        return None


@router.get("/tank-statuses", response_model=GenericResponse)
def get_tank_statuses():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT status_id, status_name, description, created_at, updated_at FROM tank_status ORDER BY status_id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


@router.get("/products", response_model=GenericResponse)
def get_products():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT product_id, product_name, description, created_at, updated_at FROM product_master ORDER BY product_id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


@router.get("/inspection-types", response_model=GenericResponse)
def get_inspection_types():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT inspection_type_id, inspection_type_name, description, created_at, updated_at FROM inspection_type ORDER BY inspection_type_id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


@router.get("/locations", response_model=GenericResponse)
def get_locations():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT location_id, location_name, description, created_at, updated_at FROM location_master ORDER BY location_id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


# ============================================================================
# Helper Functions for Tank Inspection Creation
# ============================================================================

def generate_report_number(db: Session, inspection_date: datetime) -> str:
    """
    Generate a unique report number with format: SG-T1-DDMMYYYY-XX
    where XX is a two-digit counter for the same day.
    
    Args:
        db: Database session
        inspection_date: The inspection date to use for generation
    
    Returns:
        Formatted report number string
    
    Raises:
        RuntimeError: If unable to generate unique report number after retries
    """
    date_str = inspection_date.strftime("%d%m%Y")
    
    for attempt in range(3):  # Retry up to 3 times for race conditions
        # Count existing records for this date
        count = db.query(func.count(TankInspectionDetails.inspection_id)).filter(
            func.date(TankInspectionDetails.inspection_date) == inspection_date.date()
        ).scalar() or 0
        
        next_counter = count + 1
        report_number = f"SG-T1-{date_str}-{next_counter:02d}"
        
        # Check if this report_number already exists (race condition check)
        existing = db.query(TankInspectionDetails).filter(
            TankInspectionDetails.report_number == report_number
        ).first()
        
        if not existing:
            return report_number
        
        # If we got here, there was a race condition; retry
        logger.warning(f"Report number collision for {report_number}, retrying...")
    
    raise RuntimeError(f"Unable to generate unique report number after retries for date {date_str}")


def fetch_tank_details(db: Session, tank_number: str):
    """
    Fetch tank_details record by tank_number.
    
    Args:
        db: Database session
        tank_number: Tank identifier
    
    Returns:
        Dictionary with tank detail fields
    
    Raises:
        HTTPException: 400 if tank_details not found
    """
    result = db.execute(
        text("""
            SELECT working_pressure, frame_type, design_temperature, cabinet_type, mfgr, lease
            FROM tank_details
            WHERE tank_number = :tank_number
        """),
        {"tank_number": tank_number}
    ).fetchone()
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tank details not found for tank_number: {tank_number}"
        )
    
    return {
        "working_pressure": result[0],
        "frame_type": result[1],
        "design_temperature": result[2],
        "cabinet_type": result[3],
        "mfgr": result[4],
        "lease": result[5],
    }


@router.get("/safety-valve/brands", response_model=GenericResponse)
def get_safety_valve_brands():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT id, brand_name, description, created_at, updated_at FROM safety_valve_brand ORDER BY id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


@router.get("/safety-valve/models", response_model=GenericResponse)
def get_safety_valve_models():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT id, model_name, description, created_at, updated_at FROM safety_valve_model ORDER BY id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


@router.get("/safety-valve/sizes", response_model=GenericResponse)
def get_safety_valve_sizes():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT id, size_label, description, created_at, updated_at FROM safety_valve_size ORDER BY id ASC")
            rows = cursor.fetchall()
            return {"success": True, "data": rows_to_list(rows)}
    finally:
        conn.close()


def validate_tank_exists(db: Session, tank_number: str):
    """
    Validate that tank_number exists in tank_header.
    
    Args:
        db: Database session
        tank_number: Tank identifier
    
    Raises:
        HTTPException: 400 if tank not found
    """
    result = db.execute(
        text("SELECT 1 FROM tank_header WHERE tank_number = :tank_number"),
        {"tank_number": tank_number}
    ).fetchone()
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tank not existing: {tank_number}"
        )


# ============================================================================
# Endpoint: Create Tank Inspection
# ============================================================================

@router.post("/create/tank_inspection", response_model=TankInspectionResponse, status_code=status.HTTP_201_CREATED)
def create_tank_inspection(
    payload: TankInspectionCreate,
    db: Session = Depends(get_db)
):
    """
    Create a new tank inspection record.
    
    Flow:
    1. Validate tank_number exists in tank_header
    2. Lookup and resolve master table IDs from names (status, inspection_type, product, location)
    3. Auto-fill fields from tank_details and tank_mobile
    4. Generate unique report_number with format SG-T1-DDMMYYYY-XX
    5. Insert new record and return created row
    
    Args:
        payload: TankInspectionCreate request schema
        db: Database session (injected)
    
    Returns:
        TankInspectionResponse with created record details
    
    Raises:
        HTTPException: 400 for validation errors, 500 for server errors
    """
    
    try:
        # Step 1: Validate tank exists
        validate_tank_exists(db, payload.tank_number)
        
        # Step 2: Lookup and resolve master table IDs
        # Lookup status
        status_result = db.execute(
            text("SELECT status_id FROM tank_status WHERE LOWER(status_name) = LOWER(:status_name)"),
            {"status_name": payload.status_name}
        ).fetchone()
        
        if not status_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid tank status: '{payload.status_name}' not found"
            )
        status_id = status_result[0]
        
        # Lookup inspection type
        inspection_type_result = db.execute(
            text("SELECT inspection_type_id FROM inspection_type WHERE LOWER(inspection_type_name) = LOWER(:inspection_type_name)"),
            {"inspection_type_name": payload.inspection_type_name}
        ).fetchone()
        
        if not inspection_type_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid inspection type: '{payload.inspection_type_name}' not found"
            )
        inspection_type_id = inspection_type_result[0]
        
        # Lookup product
        product_result = db.execute(
            text("SELECT product_id FROM product_master WHERE LOWER(product_name) = LOWER(:product_name)"),
            {"product_name": payload.product_name}
        ).fetchone()
        
        if not product_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid product: '{payload.product_name}' not found"
            )
        product_id = product_result[0]
        
        # Lookup location
        location_result = db.execute(
            text("SELECT location_id FROM location_master WHERE LOWER(location_name) = LOWER(:location_name)"),
            {"location_name": payload.location_name}
        ).fetchone()
        
        if not location_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid location: '{payload.location_name}' not found"
            )
        location_id = location_result[0]
        
        # Step 3: Auto-fill from tank_details
        tank_details = fetch_tank_details(db, payload.tank_number)
        
        
        # Step 5: Generate report_number
        inspection_date = payload.inspection_date or datetime.now()
        report_number = generate_report_number(db, inspection_date)
        
        # Step 6: Create new inspection record
        pi_next_date = fetch_pi_next_inspection_date(db, payload.tank_number)

        new_inspection = TankInspectionDetails(
            inspection_date=inspection_date,
            report_number=report_number,
            tank_number=payload.tank_number,
            status_id=status_id,
            product_id=product_id,
            inspection_type_id=inspection_type_id,
            location_id=location_id,
            working_pressure=tank_details.get("working_pressure"),
            frame_type=tank_details.get("frame_type"),
            design_temperature=tank_details.get("design_temperature"),
            cabinet_type=tank_details.get("cabinet_type"),
            mfgr=tank_details.get("mfgr"),
             pi_next_inspection_date=pi_next_date,
            # Safety valve fields are provided by client and are independent from tank_mobile
            safety_valve_brand=payload.safety_valve_brand,
            safety_valve_model=payload.safety_valve_model,
            safety_valve_size=payload.safety_valve_size,
            notes=payload.notes,
            created_by=payload.created_by,
            operator_id=payload.operator_id,
            ownership=(lambda lease: ('OWN' if lease in (0, '0') else 'LEASED' if lease in (1, '1') else None))(tank_details.get('lease')),
        )
        
        # Auto-fill operator_name if operator_id is provided
        if payload.operator_id:
            try:
                user = db.query(User).filter(User.emp_id == payload.operator_id).first()
                if user:
                    new_inspection.operator_name = user.name
                else:
                    logger.warning(f"Operator with emp_id {payload.operator_id} not found")
            except Exception as e:
                logger.warning(f"Could not fetch operator name: {e}")
        # Ensure ownership is set (in case tank_details changed after creation)
        try:
            if new_inspection.ownership is None:
                lease_val = tank_details.get('lease')
                new_inspection.ownership = 'OWN' if lease_val in (0, '0') else 'LEASED' if lease_val in (1, '1') else None
        except Exception:
            pass
        
        db.add(new_inspection)
        db.commit()
        db.refresh(new_inspection)
        
        logger.info(f"Created inspection record: {report_number} for tank {payload.tank_number}")
        
        # Return the created record
        return TankInspectionResponse.model_validate(new_inspection)
    
    except HTTPException:
        # Re-raise HTTPException as-is
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating tank inspection: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


# ============================================================================
# Update Tank Inspection Endpoint
# ============================================================================

class TankInspectionUpdate(BaseModel):
    """Request schema for updating a tank inspection record (supports partial updates)."""
    notes: Optional[str] = None
    operator_id: Optional[int] = None


@router.put("/update/{inspection_id}", response_model=TankInspectionResponse)
def update_tank_inspection(
    inspection_id: int,
    payload: TankInspectionUpdate,
    db: Session = Depends(get_db)
):
    """
    Update a tank inspection record by inspection_id.
    
    - **inspection_id**: The ID of the inspection to update (path parameter)
    - **payload**: Update fields (notes, operator_id)
    - When operator_id is provided, operator_name is auto-filled from users table
    
    Returns the updated inspection record.
    """
    try:
        # Fetch the inspection record
        inspection = db.query(TankInspectionDetails).filter(
            TankInspectionDetails.inspection_id == inspection_id
        ).first()
        
        if not inspection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Inspection with ID {inspection_id} not found"
            )
        
        # Update notes if provided
        if payload.notes is not None:
            inspection.notes = payload.notes
        
        # Update operator_id and auto-fill operator_name if provided
        if payload.operator_id is not None:
            inspection.operator_id = payload.operator_id
            
            # Auto-fetch operator_name from users table
            try:
                user = db.query(User).filter(User.emp_id == payload.operator_id).first()
                if user:
                    inspection.operator_name = user.name
                    logger.info(f"Set operator {user.name} ({payload.operator_id}) for inspection {inspection_id}")
                else:
                    logger.warning(f"Operator with emp_id {payload.operator_id} not found for inspection {inspection_id}")
                    inspection.operator_name = None
            except Exception as e:
                logger.warning(f"Could not fetch operator name for inspection {inspection_id}: {e}")

        # Recompute ownership from tank_details.lease for this tank
        try:
            lease_row = db.execute(
                text("SELECT lease FROM tank_details WHERE tank_number = :tank_number"),
                {"tank_number": inspection.tank_number}
            ).fetchone()
            lease_val = lease_row[0] if lease_row else None
            inspection.ownership = 'OWN' if lease_val in (0, '0') else 'LEASED' if lease_val in (1, '1') else None
        except Exception as e:
            logger.warning(f"Could not recompute ownership for inspection {inspection_id}: {e}")

        # Refresh PI next inspection date
        inspection.pi_next_inspection_date = fetch_pi_next_inspection_date(db, inspection.tank_number)
        
        # Update timestamp
        inspection.updated_at = datetime.utcnow()
        
        db.commit()
        db.refresh(inspection)
        
        logger.info(f"Updated inspection record {inspection_id}")
        
        return TankInspectionResponse.model_validate(inspection)
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating tank inspection {inspection_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


# ============================================================================
# Listing and Delete endpoints
# ============================================================================


@router.get("/list", response_model=GenericResponse)
def list_tank_inspections(inspection_id: Optional[int] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, db: Session = Depends(get_db)):
    """Return inspection listing by inspection_id (Date, Tank Number, Inspection Type, Operator Name, Status).
    Optional `inspection_id` to filter by specific inspection.
    Optional `start_date` and `end_date` filter by inspection date (YYYY-MM-DD).
    """
    try:
        params = {"inspection_id": inspection_id, "start_date": start_date, "end_date": end_date}
        sql = text(
            """
            SELECT tid.inspection_id, DATE(tid.inspection_date) AS inspection_date, tid.tank_number,
                   it.inspection_type_name AS inspection_type, tid.operator_name AS operator_name,
                   ts.status_name AS status, tid.report_number
            FROM tank_inspection_details tid
            LEFT JOIN inspection_type it ON tid.inspection_type_id = it.inspection_type_id
            LEFT JOIN tank_status ts ON tid.status_id = ts.status_id
            WHERE (:inspection_id IS NULL OR tid.inspection_id = :inspection_id)
              AND (:start_date IS NULL OR DATE(tid.inspection_date) >= :start_date)
              AND (:end_date IS NULL OR DATE(tid.inspection_date) <= :end_date)
            ORDER BY tid.inspection_id DESC
            """
        )
        rows = db.execute(sql, params).mappings().all()
        return {"success": True, "data": [dict(r) for r in rows]}
    except Exception as e:
        logger.error(f"Error listing inspections: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete("/delete/{inspection_id}")
def delete_tank_inspection(inspection_id: int, db: Session = Depends(get_db)):
    """Delete a tank inspection by `inspection_id`. Returns success:true on deletion."""
    try:
        inspection = db.query(TankInspectionDetails).filter(TankInspectionDetails.inspection_id == inspection_id).first()
        if not inspection:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Inspection {inspection_id} not found")
        db.delete(inspection)
        db.commit()
        return {"success": True, "data": {"inspection_id": inspection_id}}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error deleting inspection {inspection_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
