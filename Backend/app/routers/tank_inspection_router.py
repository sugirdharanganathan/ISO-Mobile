

from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from datetime import datetime
import os
import uuid
try:
    from PIL import Image
except Exception:
    Image = None
import os
import uuid
from decimal import Decimal
import logging

from app.database import get_db_connection, get_db
from app.models.tank_inspection_details import TankInspectionDetails
from app.models.tank_images_model import TankImages
from app.models.inspection_checklist_model import InspectionChecklist
from app.models.to_do_list_model import ToDoList
from app.models.inspection_report_model import InspectionReport
from app.models.users_model import User
from app.schemas.tank_inspection import (
    TankInspectionCreate,
    TankInspectionResponse,
)
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tank_inspection_checklist", tags=["tank_inspection"])

# Upload directory (same as other upload handlers)
UPLOAD_DIR = "uploads"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)


def _save_lifter_file(file: UploadFile, tank_number: str) -> dict:
    """Save lifter weight image and thumbnail. Returns dict with image_path, thumbnail_path."""
    file_extension = os.path.splitext(file.filename)[1] if file.filename else ".jpg"
    unique_filename = f"{tank_number}_lifter_weight_{uuid.uuid4().hex}{file_extension}"
    tank_dir = os.path.join(UPLOAD_DIR, tank_number)
    os.makedirs(tank_dir, exist_ok=True)
    dst = os.path.join(tank_dir, unique_filename)
    # write contents
    with open(dst, "wb") as buf:
        buf.write(file.file.read())
    image_path = f"{tank_number}/{unique_filename}"

    # Generate thumbnail if Pillow available
    thumb_rel = None
    if Image is not None:
        try:
            thumb_name = f"{tank_number}_lifter_weight_{uuid.uuid4().hex}_thumb.jpg"
            thumb_path = os.path.join(tank_dir, thumb_name)
            with Image.open(dst) as img:
                img.thumbnail((200, 200))
                img.convert("RGB").save(thumb_path, format="JPEG")
            thumb_rel = f"{tank_number}/{thumb_name}"
        except Exception as e:
            print(f"Warning: thumbnail generation failed for {dst}: {e}")

    return {"image_path": image_path, "thumbnail_path": thumb_rel}


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

@router.get("/lifter_weight/{inspection_id}")
def get_lifter_weight_thumbnail(inspection_id: int, db: Session = Depends(get_db)):
    """Get the thumbnail path for the lifter weight photo for a given inspection."""
    inspection = db.query(TankInspectionDetails).filter(TankInspectionDetails.inspection_id == inspection_id).first()
    if not inspection or not inspection.lifter_weight:
        raise HTTPException(status_code=404, detail="No lifter weight photo found for this inspection.")

    rel_path = inspection.lifter_weight
    folder = os.path.dirname(rel_path)
    tank_number = folder
    folder_abs = os.path.join(UPLOAD_DIR, folder)
    thumb = None
    if os.path.isdir(folder_abs):
        # Find any file matching <tank_number>_lifter_weight_*_thumb.jpg
        candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")]
        if candidates:
            # If multiple, pick the most recently modified
            candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
            thumb = f"{folder}/{candidates[0]}"
    return {"inspection_id": inspection_id, "thumbnail_path": thumb}
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


@router.post("/{inspection_id}/lifter_weight")
def upload_lifter_weight(inspection_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Upload lifter weight photo for an inspection and store path in `lifter_weight` column.

    - `inspection_id` path param identifies the inspection record.
    - Request must be multipart/form-data with `file`.
    """
    try:
        # Fetch inspection
        inspection = db.query(TankInspectionDetails).filter(TankInspectionDetails.inspection_id == inspection_id).first()
        if not inspection:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Inspection {inspection_id} not found")

        # Basic validation
        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File must be an image")

        # Save file

        saved = _save_lifter_file(file, inspection.tank_number)
        rel_path = saved["image_path"]
        thumb_path = saved["thumbnail_path"]

        # If there was an existing file, try to remove it and its thumbnail
        try:
            if inspection.lifter_weight:
                old_path = os.path.join(UPLOAD_DIR, *inspection.lifter_weight.split("/"))
                if os.path.exists(old_path):
                    os.remove(old_path)
                # Remove old thumbnail if present
                old_base = os.path.splitext(os.path.basename(old_path))[0]
                folder = os.path.dirname(old_path)
                if os.path.isdir(folder):
                    for fn in os.listdir(folder):
                        if 'thumb' in fn and old_base in fn:
                            try:
                                os.remove(os.path.join(folder, fn))
                            except Exception:
                                pass
        except Exception:
            pass

        # Update record
        inspection.lifter_weight = rel_path
        inspection.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(inspection)

        return {"success": True, "message": "Lifter weight photo uploaded", "data": {"inspection_id": inspection_id, "lifter_weight": rel_path, "thumbnail": thumb_path}}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error uploading lifter weight for inspection {inspection_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/review/{inspection_id}")
def get_inspection_review(inspection_id: int, db: Session = Depends(get_db)):
    """Return a review report combining tank_inspection_details, tank_images (image_type + thumbnail), inspection_checklist and to_do_list for the given inspection_id."""
    inspection = db.query(TankInspectionDetails).filter(TankInspectionDetails.inspection_id == inspection_id).first()
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    # Build inspection details dict excluding created_at/updated_at
    insp = inspection.as_dict if hasattr(inspection, 'as_dict') else None
    if insp is None:
        # fallback: construct manually
        insp = {c.name: getattr(inspection, c.name) for c in inspection.__table__.columns}
    insp.pop('created_at', None)
    insp.pop('updated_at', None)

    # lifter weight thumbnail detection
    lifter_thumb = None
    try:
        if inspection.lifter_weight:
            folder = os.path.dirname(inspection.lifter_weight)
            tank_number = folder
            folder_abs = os.path.join(UPLOAD_DIR, folder)
            if os.path.isdir(folder_abs):
                candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")]
                if candidates:
                    candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                    lifter_thumb = f"{folder}/{candidates[0]}"
    except Exception:
        lifter_thumb = None
    insp['lifter_weight_thumbnail'] = lifter_thumb

    # Images: find images for tank and date
    images_out = []
    try:
        tank_number = inspection.tank_number
        insp_date = inspection.inspection_date.date() if inspection.inspection_date else None

        # Attempt to load DB records for images (if any) to keep behaviour consistent,
        # but ultimately we will scan the uploads folder for available thumbnails grouped by image_type.
        try:
            if insp_date:
                imgs = db.query(TankImages).filter(TankImages.tank_number == tank_number, TankImages.created_date == insp_date).all()
            else:
                imgs = db.query(TankImages).filter(TankImages.tank_number == tank_number).all()
        except Exception:
            imgs = []

        # Only include image_types present in the tank_images table for this inspection (exclude lifter_weight)
        # For each image_type, find the most recent thumbnail or return null if not present
        # Get all tank_images for this inspection (by tank_number and created_date)
        if insp_date:
            imgs = db.query(TankImages).filter(TankImages.tank_number == tank_number, TankImages.created_date == insp_date).all()
        else:
            imgs = db.query(TankImages).filter(TankImages.tank_number == tank_number).all()

        # For each image row (excluding lifter_weight), find the thumbnail or set null
        tank_images_list = []
        folder_abs = os.path.join(UPLOAD_DIR, tank_number)
        for im in imgs:
            img_type = im.image_type
            if not img_type or img_type.lower() == "lifter_weight":
                continue
            thumb_path = None
            if os.path.isdir(folder_abs):
                # Find any file matching <tank_number>_<image_type>_*_thumb.jpg
                prefix = f"{tank_number}_{img_type}_"
                candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(prefix) and fn.endswith("_thumb.jpg")]
                if candidates:
                    # Pick the most recently modified
                    candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                    thumb_path = f"{tank_number}/{candidates[0]}"
            tank_images_list.append({"image_type": img_type, "thumbnail_path": thumb_path})
    except Exception:
        images_out = []

    # Find related inspection_report for checklist and todo using tank_number + date
    checklist_out = []
    todo_out = []
    try:
        report = None
        if inspection.inspection_date and inspection.tank_number:
            insp_date_str = inspection.inspection_date.date().isoformat()
            report = db.query(InspectionReport).filter(InspectionReport.tank_number == inspection.tank_number, InspectionReport.inspection_date == insp_date_str).first()
        if report:
            report_id = report.id
            # fetch checklist items
            rows = db.query(InspectionChecklist).filter(InspectionChecklist.report_id == report_id).all()
            for r in rows:
                checklist_out.append({"job_name": r.job_name, "sub_job_name": r.sub_job_description, "status": r.status, "comment": r.comment})

            # fetch todo items
            todos = db.query(ToDoList).filter(ToDoList.report_id == report_id).all()
            for t in todos:
                todo_out.append({"job_name": t.job_name, "sub_job_name": t.sub_job_description, "status": t.status, "comment": t.comment})
    except Exception:
        checklist_out = []
        todo_out = []

    # include tank_images_list for convenience (may be empty)
    try:
        resp = {"inspection": insp, "images": images_out, "tank_images": tank_images_list, "inspection_checklist": checklist_out, "to_do_list": todo_out}
    except Exception:
        resp = {"inspection": insp, "images": images_out, "tank_images": [], "inspection_checklist": checklist_out, "to_do_list": todo_out}

    return resp


class ReviewUpdateModel(BaseModel):
    inspection: Optional[dict] = None
    checklist: Optional[List[dict]] = None
    to_do: Optional[List[dict]] = None


@router.put("/review/{inspection_id}")
def update_inspection_review(inspection_id: int, payload: ReviewUpdateModel, db: Session = Depends(get_db)):
    """Update inspection details, checklist items and to-do items for a review. Images are not handled here."""
    inspection = db.query(TankInspectionDetails).filter(TankInspectionDetails.inspection_id == inspection_id).first()
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    try:
        # Update inspection fields
        if payload.inspection:
            for k, v in payload.inspection.items():
                if k in ("created_at", "updated_at", "inspection_id"):
                    continue
                if hasattr(inspection, k):
                    setattr(inspection, k, v)

        # Find related report if exists
        report = None
        if inspection.inspection_date and inspection.tank_number:
            insp_date_str = inspection.inspection_date.date().isoformat()
            report = db.query(InspectionReport).filter(InspectionReport.tank_number == inspection.tank_number, InspectionReport.inspection_date == insp_date_str).first()

        # Update checklist items
        if payload.checklist and report:
            for item in payload.checklist:
                # require either id or sn
                if 'id' in item and item['id']:
                    chk = db.query(InspectionChecklist).filter(InspectionChecklist.id == item['id']).first()
                else:
                    # try match by report_id and sn if provided
                    chk = None
                    if 'sn' in item:
                        chk = db.query(InspectionChecklist).filter(InspectionChecklist.report_id == report.id, InspectionChecklist.sn == item['sn']).first()
                if not chk:
                    continue
                if 'job_name' in item:
                    chk.job_name = item['job_name']
                if 'sub_job_name' in item:
                    chk.sub_job_description = item['sub_job_name']
                if 'status' in item:
                    chk.status = item['status']
                if 'comment' in item:
                    chk.comment = item['comment']
                # flagged sync
                chk.flagged = bool(chk.comment and str(chk.comment).strip() != "")

                # sync to to_do_list
                if chk.flagged:
                    # upsert to to_do_list
                    existing = db.query(ToDoList).filter(ToDoList.checklist_id == chk.id).first()
                    if existing:
                        existing.job_name = chk.job_name
                        existing.sub_job_description = chk.sub_job_description
                        existing.status = chk.status
                        existing.comment = chk.comment
                    else:
                        nd = ToDoList()
                        nd.checklist_id = chk.id
                        nd.report_id = chk.report_id
                        nd.tank_number = chk.tank_number
                        nd.job_name = chk.job_name
                        nd.sub_job_description = chk.sub_job_description
                        nd.sn = chk.sn
                        nd.status = chk.status
                        nd.comment = chk.comment
                        db.add(nd)
                else:
                    # remove from to_do_list if exists
                    db.query(ToDoList).filter(ToDoList.checklist_id == chk.id).delete()

        # Update to_do items directly
        if payload.to_do and report:
            for t in payload.to_do:
                if 'id' in t and t['id']:
                    td = db.query(ToDoList).filter(ToDoList.id == t['id']).first()
                    if not td:
                        continue
                    if 'job_name' in t:
                        td.job_name = t['job_name']
                    if 'sub_job_name' in t:
                        td.sub_job_description = t['sub_job_name']
                    if 'status' in t:
                        td.status = t['status']
                    if 'comment' in t:
                        td.comment = t['comment']

        db.commit()
        db.refresh(inspection)
        return {"success": True, "message": "Review updated"}
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating review for {inspection_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/review/{inspection_id}")
def delete_inspection_review(inspection_id: int, db: Session = Depends(get_db)):
    """Delete an inspection and associated report, checklist and to-do entries. Does not delete images."""
    inspection = db.query(TankInspectionDetails).filter(TankInspectionDetails.inspection_id == inspection_id).first()
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    try:
        report = None
        if inspection.inspection_date and inspection.tank_number:
            insp_date_str = inspection.inspection_date.date().isoformat()
            report = db.query(InspectionReport).filter(InspectionReport.tank_number == inspection.tank_number, InspectionReport.inspection_date == insp_date_str).first()
        if report:
            # delete checklists and todos
            db.query(InspectionChecklist).filter(InspectionChecklist.report_id == report.id).delete()
            db.query(ToDoList).filter(ToDoList.report_id == report.id).delete()
            db.delete(report)

        db.delete(inspection)
        db.commit()
        return {"success": True, "message": "Inspection and related report/checklist/to-do entries deleted"}
    except Exception as e:
        db.rollback()
        logger.error(f"Error deleting review for {inspection_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
