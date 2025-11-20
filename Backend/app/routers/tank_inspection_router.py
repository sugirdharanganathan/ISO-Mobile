from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError  # ✅ import this
import os
import uuid

try:
    from PIL import Image
except Exception:
    Image = None

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


# ============================================================================
# Helper Functions for Tank Inspection Creation
# ============================================================================


def generate_report_number(db: Session, inspection_date: datetime) -> str:
    """
    Generate a unique report number with format: SG-T1-DDMMYYYY-XX
    where XX is a two-digit counter for the same day.
    """
    date_str = inspection_date.strftime("%d%m%Y")

    for attempt in range(3):  # Retry up to 3 times for race conditions
        # Count existing records for this date
        count = (
            db.query(func.count(TankInspectionDetails.inspection_id))
            .filter(func.date(TankInspectionDetails.inspection_date) == inspection_date.date())
            .scalar()
            or 0
        )

        next_counter = count + 1
        report_number = f"SG-T1-{date_str}-{next_counter:02d}"

        # Check if this report_number already exists (race condition check)
        existing = (
            db.query(TankInspectionDetails)
            .filter(TankInspectionDetails.report_number == report_number)
            .first()
        )

        if not existing:
            return report_number

        # If we got here, there was a race condition; retry
        logger.warning(f"Report number collision for {report_number}, retrying...")

    raise RuntimeError(f"Unable to generate unique report number after retries for date {date_str}")


def fetch_tank_details(db: Session, tank_number: str):
    """
    Fetch tank_details record by tank_number.

    Returns:
        Dictionary with tank detail fields (Decimal values converted to float)
    """
    result = db.execute(
        text(
            """
            SELECT working_pressure, frame_type, design_temperature, cabinet_type, mfgr, lease
            FROM tank_details
            WHERE tank_number = :tank_number
            """
        ),
        {"tank_number": tank_number},
    ).fetchone()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tank details not found for tank_number: {tank_number}",
        )

    working_pressure, frame_type, design_temperature, cabinet_type, mfgr, lease = result

    def to_float_if_decimal(val):
        if isinstance(val, Decimal):
            return float(val)
        return val

    return {
        "working_pressure": to_float_if_decimal(working_pressure),
        "frame_type": frame_type,
        "design_temperature": to_float_if_decimal(design_temperature),
        "cabinet_type": cabinet_type,
        "mfgr": mfgr,
        "lease": lease,
    }


def build_tank_inspection_response(entity: TankInspectionDetails) -> TankInspectionResponse:
    """
    Convert TankInspectionDetails ORM object into TankInspectionResponse,
    casting any Decimal values to float so Pydantic can validate against
    float fields (or coerce to string if your schema still uses str).
    """
    data = {}
    for col in entity.__table__.columns:
        val = getattr(entity, col.name)
        if isinstance(val, Decimal):
            data[col.name] = float(val)
        else:
            data[col.name] = val
    return TankInspectionResponse.model_validate(data)


# Consolidated masters response models
class TankStatusSchema(BaseModel):
    status_id: Optional[int]
    status_name: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class ProductSchema(BaseModel):
    product_id: Optional[int]
    product_name: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class InspectionTypeSchema(BaseModel):
    inspection_type_id: Optional[int]
    inspection_type_name: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class LocationSchema(BaseModel):
    location_id: Optional[int]
    location_name: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class SafetyValveBrandSchema(BaseModel):
    id: Optional[int]
    brand_name: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class SafetyValveModelSchema(BaseModel):
    id: Optional[int]
    model_name: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class SafetyValveSizeSchema(BaseModel):
    id: Optional[int]
    size_label: Optional[str]
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class TankInspectionMastersResponse(BaseModel):
    tank_statuses: List[TankStatusSchema]
    products: List[ProductSchema]
    inspection_types: List[InspectionTypeSchema]
    locations: List[LocationSchema]
    safety_valve_brands: List[SafetyValveBrandSchema]
    safety_valve_models: List[SafetyValveModelSchema]
    safety_valve_sizes: List[SafetyValveSizeSchema]


@router.get("/masters", response_model=TankInspectionMastersResponse, summary="Get all tank inspection master data")
def get_all_tank_inspection_masters():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute(
                "SELECT status_id, status_name, description, created_at, updated_at "
                "FROM tank_status ORDER BY status_id ASC"
            )
            tank_statuses = cursor.fetchall()

            cursor.execute(
                "SELECT product_id, product_name, description, created_at, updated_at "
                "FROM product_master ORDER BY product_id ASC"
            )
            products = cursor.fetchall()

            cursor.execute(
                "SELECT inspection_type_id, inspection_type_name, description, created_at, updated_at "
                "FROM inspection_type ORDER BY inspection_type_id ASC"
            )
            inspection_types = cursor.fetchall()

            cursor.execute(
                "SELECT location_id, location_name, description, created_at, updated_at "
                "FROM location_master ORDER BY location_id ASC"
            )
            locations = cursor.fetchall()

            cursor.execute(
                "SELECT id, brand_name, description, created_at, updated_at "
                "FROM safety_valve_brand ORDER BY id ASC"
            )
            safety_valve_brands = cursor.fetchall()

            cursor.execute(
                "SELECT id, model_name, description, created_at, updated_at "
                "FROM safety_valve_model ORDER BY id ASC"
            )
            safety_valve_models = cursor.fetchall()

            cursor.execute(
                "SELECT id, size_label, description, created_at, updated_at "
                "FROM safety_valve_size ORDER BY id ASC"
            )
            safety_valve_sizes = cursor.fetchall()

            return {
                "tank_statuses": tank_statuses,
                "products": products,
                "inspection_types": inspection_types,
                "locations": locations,
                "safety_valve_brands": safety_valve_brands,
                "safety_valve_models": safety_valve_models,
                "safety_valve_sizes": safety_valve_sizes,
            }
    finally:
        conn.close()


def validate_tank_exists(db: Session, tank_number: str):
    """
    Validate that tank_number exists in tank_header.
    """
    result = db.execute(
        text("SELECT 1 FROM tank_header WHERE tank_number = :tank_number"),
        {"tank_number": tank_number},
    ).fetchone()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tank not existing: {tank_number}",
        )


# ---------------------------------------------------------------------------
# NEW: Active tanks endpoint (tank_id + tank_number from tank_details where status='active')
# ---------------------------------------------------------------------------


class ActiveTankSchema(BaseModel):
    tank_id: int
    tank_number: str


@router.get("/active-tanks", response_model=List[ActiveTankSchema])
def get_active_tanks(db: Session = Depends(get_db)):
    try:
        rows = (
            db.execute(text("SELECT tank_id, tank_number FROM tank_details WHERE status = 'active'"))
            .mappings()
            .all()
        )
        return [ActiveTankSchema(tank_id=row["tank_id"], tank_number=row["tank_number"]) for row in rows]
    except Exception as e:
        logger.error(f"Error fetching active tanks: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error fetching active tanks",
        )


@router.get("/lifter_weight/{inspection_id}")
def get_lifter_weight_thumbnail(inspection_id: int, db: Session = Depends(get_db)):
    """Get the thumbnail path for the lifter weight photo for a given inspection."""
    inspection = (
        db.query(TankInspectionDetails)
        .filter(TankInspectionDetails.inspection_id == inspection_id)
        .first()
    )
    if not inspection or not inspection.lifter_weight:
        raise HTTPException(status_code=404, detail="No lifter weight photo found for this inspection.")

    rel_path = inspection.lifter_weight
    folder = os.path.dirname(rel_path)
    tank_number = folder
    folder_abs = os.path.join(UPLOAD_DIR, folder)
    thumb = None
    if os.path.isdir(folder_abs):
        # Find any file matching <tank_number>_lifter_weight_*_thumb.jpg
        candidates = [
            fn
            for fn in os.listdir(folder_abs)
            if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")
        ]
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
    db: Session = Depends(get_db),
):
    """
    Create a new tank inspection record.
    Also ensures a row exists in inspection_report for this tank & date.
    """
    try:
        # Step 1: Validate tank exists
        validate_tank_exists(db, payload.tank_number)

        # Step 2: Lookup and resolve master table IDs
        # ----- status -----
        status_result = db.execute(
            text("SELECT status_id FROM tank_status WHERE LOWER(status_name) = LOWER(:status_name)"),
            {"status_name": payload.status_name},
        ).fetchone()
        if not status_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid tank status: '{payload.status_name}' not found",
            )
        status_id = status_result[0]

        # ----- inspection type -----
        inspection_type_result = db.execute(
            text(
                "SELECT inspection_type_id FROM inspection_type "
                "WHERE LOWER(inspection_type_name) = LOWER(:inspection_type_name)"
            ),
            {"inspection_type_name": payload.inspection_type_name},
        ).fetchone()
        if not inspection_type_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid inspection type: '{payload.inspection_type_name}' not found",
            )
        inspection_type_id = inspection_type_result[0]

        # ----- product -----
        product_result = db.execute(
            text("SELECT product_id FROM product_master WHERE LOWER(product_name) = LOWER(:product_name)"),
            {"product_name": payload.product_name},
        ).fetchone()
        if not product_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid product: '{payload.product_name}' not found",
            )
        product_id = product_result[0]

        # ----- location -----
        location_result = db.execute(
            text("SELECT location_id FROM location_master WHERE LOWER(location_name) = LOWER(:location_name)"),
            {"location_name": payload.location_name},
        ).fetchone()
        if not location_result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid location: '{payload.location_name}' not found",
            )
        location_id = location_result[0]

        # Step 3: Auto-fill from tank_details
        tank_details = fetch_tank_details(db, payload.tank_number)

        # Step 4: inspection_date (always "now")
        inspection_date = datetime.now()

        # Step 4a: ensure inspection_report row exists for this tank & date
        insp_date_str = inspection_date.date().isoformat()
        existing_report = (
            db.query(InspectionReport)
            .filter(
                InspectionReport.tank_number == payload.tank_number,
                InspectionReport.inspection_date == insp_date_str,
            )
            .first()
        )

        if not existing_report:
            new_report = InspectionReport(
                tank_number=payload.tank_number,
                inspection_date=insp_date_str,  # DATE column / string "YYYY-MM-DD"
                emp_id=payload.operator_id,
                notes=payload.notes,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(new_report)
            # no need to flush id here unless you want it; linkage is by tank+date

        # Prevent duplicate inspection for same tank + date + inspection_type
        existing = (
            db.query(TankInspectionDetails)
            .filter(
                TankInspectionDetails.tank_number == payload.tank_number,
                func.date(TankInspectionDetails.inspection_date) == inspection_date.date(),
                TankInspectionDetails.inspection_type_id == inspection_type_id,
            )
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Inspection already exists for tank {payload.tank_number} "
                    f"on {inspection_date.date()} with inspection type '{payload.inspection_type_name}' "
                    f"(inspection_id={existing.inspection_id})."
                ),
            )

        # Step 5: Generate report_number
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
            safety_valve_brand=payload.safety_valve_brand,
            safety_valve_model=payload.safety_valve_model,
            safety_valve_size=payload.safety_valve_size,
            notes=payload.notes,
            created_by=payload.created_by,
            operator_id=payload.operator_id,
            ownership=(
                "OWN"
                if tank_details.get("lease") in (0, "0")
                else "LEASED"
                if tank_details.get("lease") in (1, "1")
                else None
            ),
        )

        # operator_name auto-fill (unchanged)
        if payload.operator_id:
            try:
                user = db.query(User).filter(User.emp_id == payload.operator_id).first()
                if user:
                    new_inspection.operator_name = user.name
                else:
                    logger.warning(f"Operator with emp_id {payload.operator_id} not found")
            except Exception as e:
                logger.warning(f"Could not fetch operator name: {e}")
        try:
            if new_inspection.ownership is None:
                lease_val = tank_details.get("lease")
                new_inspection.ownership = (
                    "OWN" if lease_val in (0, "0") else "LEASED" if lease_val in (1, "1") else None
                )
        except Exception:
            pass

        db.add(new_inspection)
        db.commit()
        db.refresh(new_inspection)

        logger.info(f"Created inspection record: {report_number} for tank {payload.tank_number}")

        return build_tank_inspection_response(new_inspection)

    except HTTPException:
        raise
    except IntegrityError:
        db.rollback()
        # In case the DB unique constraint catches a duplicate first
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Duplicate inspection for this tank, date and inspection type.",
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating tank inspection: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


# ============================================================================
# Update Tank Inspection Endpoint
# ============================================================================

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


@router.put("/update/{inspection_id}", response_model=TankInspectionResponse)
def update_tank_inspection(
    inspection_id: int,
    payload: TankInspectionUpdate,
    db: Session = Depends(get_db),
):
    """
    Update a tank inspection record by inspection_id.

    All fields in TankInspectionUpdate are optional.
    Only the ones you send in the body will be changed.
    """
    try:
        # Fetch the inspection record
        inspection = (
            db.query(TankInspectionDetails)
            .filter(TankInspectionDetails.inspection_id == inspection_id)
            .first()
        )

        if not inspection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Inspection with ID {inspection_id} not found",
            )

        # --------- basic fields ---------
        if payload.inspection_date is not None:
            inspection.inspection_date = payload.inspection_date

        if payload.tank_number is not None:
            if payload.tank_number != inspection.tank_number:
                validate_tank_exists(db, payload.tank_number)
            inspection.tank_number = payload.tank_number

        # --------- status (id or name) ---------
        if payload.status_id is not None:
            inspection.status_id = payload.status_id
        elif payload.status_name is not None:
            row = db.execute(
                text("SELECT status_id FROM tank_status WHERE LOWER(status_name) = LOWER(:name)"),
                {"name": payload.status_name},
            ).fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid tank status: '{payload.status_name}' not found",
                )
            inspection.status_id = row[0]

        # --------- inspection type ---------
        if payload.inspection_type_id is not None:
            inspection.inspection_type_id = payload.inspection_type_id
        elif payload.inspection_type_name is not None:
            row = db.execute(
                text(
                    "SELECT inspection_type_id FROM inspection_type "
                    "WHERE LOWER(inspection_type_name) = LOWER(:name)"
                ),
                {"name": payload.inspection_type_name},
            ).fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid inspection type: '{payload.inspection_type_name}' not found",
                )
            inspection.inspection_type_id = row[0]

        # --------- product ---------
        if payload.product_id is not None:
            inspection.product_id = payload.product_id
        elif payload.product_name is not None:
            row = db.execute(
                text("SELECT product_id FROM product_master WHERE LOWER(product_name) = LOWER(:name)"),
                {"name": payload.product_name},
            ).fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid product: '{payload.product_name}' not found",
                )
            inspection.product_id = row[0]

        # --------- location ---------
        if payload.location_id is not None:
            inspection.location_id = payload.location_id
        elif payload.location_name is not None:
            row = db.execute(
                text("SELECT location_id FROM location_master WHERE LOWER(location_name) = LOWER(:name)"),
                {"name": payload.location_name},
            ).fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid location: '{payload.location_name}' not found",
                )
            inspection.location_id = row[0]

        # --------- tank detail overrides (optional) ---------
        if payload.working_pressure is not None:
            inspection.working_pressure = payload.working_pressure
        if payload.frame_type is not None:
            inspection.frame_type = payload.frame_type
        if payload.design_temperature is not None:
            inspection.design_temperature = payload.design_temperature
        if payload.cabinet_type is not None:
            inspection.cabinet_type = payload.cabinet_type
        if payload.mfgr is not None:
            inspection.mfgr = payload.mfgr

        # --------- safety valve fields ---------
        if payload.safety_valve_brand is not None:
            inspection.safety_valve_brand = payload.safety_valve_brand
        if payload.safety_valve_model is not None:
            inspection.safety_valve_model = payload.safety_valve_model
        if payload.safety_valve_size is not None:
            inspection.safety_valve_size = payload.safety_valve_size

        # --------- notes ---------
        if payload.notes is not None:
            inspection.notes = payload.notes

        # --------- operator (and name lookup) ---------
        if payload.operator_id is not None:
            inspection.operator_id = payload.operator_id

            try:
                user = db.query(User).filter(User.emp_id == payload.operator_id).first()
                if user:
                    inspection.operator_name = user.name
                    logger.info(f"Set operator {user.name} ({payload.operator_id}) for inspection {inspection_id}")
                else:
                    logger.warning(
                        f"Operator with emp_id {payload.operator_id} not found for inspection {inspection_id}"
                    )
                    inspection.operator_name = None
            except Exception as e:
                logger.warning(f"Could not fetch operator name for inspection {inspection_id}: {e}")

        # --------- ownership (optional override, otherwise recompute) ---------
        if payload.ownership is not None:
            inspection.ownership = payload.ownership
        else:
            try:
                lease_row = db.execute(
                    text("SELECT lease FROM tank_details WHERE tank_number = :tank_number"),
                    {"tank_number": inspection.tank_number},
                ).fetchone()
                lease_val = lease_row[0] if lease_row else None
                inspection.ownership = (
                    "OWN" if lease_val in (0, "0") else "LEASED" if lease_val in (1, "1") else None
                )
            except Exception as e:
                logger.warning(f"Could not recompute ownership for inspection {inspection_id}: {e}")

        # --------- refresh PI next inspection date ---------
        inspection.pi_next_inspection_date = fetch_pi_next_inspection_date(db, inspection.tank_number)

        # --------- timestamp ---------
        inspection.updated_at = datetime.utcnow()

        db.commit()
        db.refresh(inspection)

        logger.info(f"Updated inspection record {inspection_id}")

        return build_tank_inspection_response(inspection)

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating tank inspection {inspection_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


# ============================================================================
# Listing and Delete endpoints
# ============================================================================


@router.get("/list", response_model=GenericResponse)
def list_tank_inspections(
    inspection_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return inspection listing."""
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
    """Delete a tank inspection by inspection_id."""
    try:
        inspection = (
            db.query(TankInspectionDetails)
            .filter(TankInspectionDetails.inspection_id == inspection_id)
            .first()
        )
        if not inspection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Inspection {inspection_id} not found",
            )
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
    """Upload lifter weight photo for an inspection and store path in lifter_weight column."""
    try:
        # Fetch inspection
        inspection = (
            db.query(TankInspectionDetails)
            .filter(TankInspectionDetails.inspection_id == inspection_id)
            .first()
        )
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
                        if "thumb" in fn and old_base in fn:
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

        return {
            "success": True,
            "message": "Lifter weight photo uploaded",
            "data": {
                "inspection_id": inspection_id,
                "lifter_weight": rel_path,
                "thumbnail": thumb_path,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error uploading lifter weight for inspection {inspection_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/review/{inspection_id}")
def get_inspection_review(inspection_id: int, db: Session = Depends(get_db)):
    """Return a review report combining tank_inspection_details, tank_images, checklist and to_do_list."""
    inspection = (
        db.query(TankInspectionDetails)
        .filter(TankInspectionDetails.inspection_id == inspection_id)
        .first()
    )
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    # Build inspection details dict excluding created_at/updated_at
    insp = inspection.as_dict if hasattr(inspection, "as_dict") else None
    if insp is None:
        # fallback: construct manually
        insp = {c.name: getattr(inspection, c.name) for c in inspection.__table__.columns}
    insp.pop("created_at", None)
    insp.pop("updated_at", None)

    # lifter weight thumbnail detection
    lifter_thumb = None
    try:
        if inspection.lifter_weight:
            folder = os.path.dirname(inspection.lifter_weight)
            tank_number = folder
            folder_abs = os.path.join(UPLOAD_DIR, folder)
            if os.path.isdir(folder_abs):
                candidates = [
                    fn
                    for fn in os.listdir(folder_abs)
                    if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")
                ]
                if candidates:
                    candidates.sort(
                        key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)),
                        reverse=True,
                    )
                    lifter_thumb = f"{folder}/{candidates[0]}"
    except Exception:
        lifter_thumb = None
    insp["lifter_weight_thumbnail"] = lifter_thumb

    # Images: find images for tank and date
    images_out = []
    try:
        tank_number = inspection.tank_number
        insp_date = inspection.inspection_date.date() if inspection.inspection_date else None

        # Attempt to load DB records for images (if any)
        try:
            if insp_date:
                imgs = (
                    db.query(TankImages)
                    .filter(TankImages.tank_number == tank_number, TankImages.created_date == insp_date)
                    .all()
                )
            else:
                imgs = db.query(TankImages).filter(TankImages.tank_number == tank_number).all()
        except Exception:
            imgs = []

        # For each image row (excluding lifter_weight), find the thumbnail or set null
        tank_images_list = []
        folder_abs = os.path.join(UPLOAD_DIR, tank_number)
        for im in imgs:
            img_type = im.image_type
            if not img_type or img_type.lower() == "lifter_weight":
                continue
            thumb_path = None
            if os.path.isdir(folder_abs):
                prefix = f"{tank_number}_{img_type}_"
                candidates = [
                    fn
                    for fn in os.listdir(folder_abs)
                    if fn.startswith(prefix) and fn.endswith("_thumb.jpg")
                ]
                if candidates:
                    candidates.sort(
                        key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)),
                        reverse=True,
                    )
                    thumb_path = f"{tank_number}/{candidates[0]}"
            tank_images_list.append({"image_type": img_type, "thumbnail_path": thumb_path})
    except Exception:
        tank_images_list = []

    # Find related inspection_report for checklist and todo using tank_number + date
    checklist_out = []
    todo_out = []
    try:
        report = None
        if inspection.inspection_date and inspection.tank_number:
            insp_date_str = inspection.inspection_date.date().isoformat()
            report = (
                db.query(InspectionReport)
                .filter(
                    InspectionReport.tank_number == inspection.tank_number,
                    InspectionReport.inspection_date == insp_date_str,
                )
                .first()
            )
        if report:
            report_id = report.id
            # fetch checklist items
            rows = db.query(InspectionChecklist).filter(InspectionChecklist.report_id == report_id).all()
            for r in rows:
                checklist_out.append(
                    {
                        "job_name": r.job_name,
                        "sub_job_name": r.sub_job_description,
                        "status": r.status,
                        "comment": r.comment,
                    }
                )

            # fetch todo items
            todos = db.query(ToDoList).filter(ToDoList.report_id == report_id).all()
            for t in todos:
                todo_out.append(
                    {
                        "job_name": t.job_name,
                        "sub_job_name": t.sub_job_description,
                        "status": t.status,
                        "comment": t.comment,
                    }
                )
    except Exception:
        checklist_out = []
        todo_out = []

    resp = {
        "inspection": insp,
        "images": images_out,
        "tank_images": tank_images_list,
        "inspection_checklist": checklist_out,
        "to_do_list": todo_out,
    }

    return resp


class ReviewUpdateModel(BaseModel):
    inspection: Optional[dict] = None
    checklist: Optional[List[dict]] = None
    to_do: Optional[List[dict]] = None


@router.put("/review/{inspection_id}")
def update_inspection_review(inspection_id: int, payload: ReviewUpdateModel, db: Session = Depends(get_db)):
    """Update inspection details, checklist items and to-do items for a review. Images are not handled here."""
    inspection = (
        db.query(TankInspectionDetails)
        .filter(TankInspectionDetails.inspection_id == inspection_id)
        .first()
    )
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
            report = (
                db.query(InspectionReport)
                .filter(
                    InspectionReport.tank_number == inspection.tank_number,
                    InspectionReport.inspection_date == insp_date_str,
                )
                .first()
            )

        # Update checklist items
        if payload.checklist and report:
            for item in payload.checklist:
                # require either id or sn
                if "id" in item and item["id"]:
                    chk = db.query(InspectionChecklist).filter(InspectionChecklist.id == item["id"]).first()
                else:
                    chk = None
                    if "sn" in item:
                        chk = (
                            db.query(InspectionChecklist)
                            .filter(
                                InspectionChecklist.report_id == report.id,
                                InspectionChecklist.sn == item["sn"],
                            )
                            .first()
                        )
                if not chk:
                    continue
                if "job_name" in item:
                    chk.job_name = item["job_name"]
                if "sub_job_name" in item:
                    chk.sub_job_description = item["sub_job_name"]
                if "status" in item:
                    chk.status = item["status"]
                if "comment" in item:
                    chk.comment = item["comment"]
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
                if "id" in t and t["id"]:
                    td = db.query(ToDoList).filter(ToDoList.id == t["id"]).first()
                    if not td:
                        continue
                    if "job_name" in t:
                        td.job_name = t["job_name"]
                    if "sub_job_name" in t:
                        td.sub_job_description = t["sub_job_name"]
                    if "status" in t:
                        td.status = t["status"]
                    if "comment" in t:
                        td.comment = t["comment"]

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
    inspection = (
        db.query(TankInspectionDetails)
        .filter(TankInspectionDetails.inspection_id == inspection_id)
        .first()
    )
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    try:
        report = None
        if inspection.inspection_date and inspection.tank_number:
            insp_date_str = inspection.inspection_date.date().isoformat()
            report = (
                db.query(InspectionReport)
                .filter(
                    InspectionReport.tank_number == inspection.tank_number,
                    InspectionReport.inspection_date == insp_date_str,
                )
                .first()
            )
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
