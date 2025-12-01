# app/routers/tank_inspection_router.py
from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File, Header
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Generator, Any
from sqlalchemy import func, text, inspect
from sqlalchemy.orm import Session
import os
import uuid
import logging
import traceback
import jwt  # PyJWT
import pymysql
from pymysql.cursors import DictCursor
from decimal import Decimal
import urllib.parse
import importlib

from app.database import get_db, get_db_connection
from app.routers import to_do_list_router
from app.routers.tank_checkpoints_router import FAULTY_STATUS_IDS

try:
    from PIL import Image
except Exception:
    Image = None

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter(prefix="/api/tank_inspection_checklist", tags=["tank_inspection"])

UPLOAD_DIR = "uploads"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)
IMAGES_ROOT_DIR = os.path.join(UPLOAD_DIR, "tank_images_mobile")
if not os.path.exists(IMAGES_ROOT_DIR):
    os.makedirs(IMAGES_ROOT_DIR, exist_ok=True)

JWT_SECRET = os.getenv("JWT_SECRET", "change_this_in_production")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


def _is_blank_or_zero(v):
    """Return True if value is None, empty string, or numeric 0 (or "0")."""
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    try:
        return int(v) == 0
    except Exception:
        return False


# Response helpers (uniform envelope)
from fastapi.encoders import jsonable_encoder

def success_resp(message: str, data: Any = None, status_code: int = 200):
    if data is None:
        data = {}
    try:
        payload = jsonable_encoder(data)
    except Exception:
        try:
            payload = jsonable_encoder(str(data))
        except Exception:
            payload = {}
    return JSONResponse(status_code=status_code, content={"success": True, "message": message, "data": payload})

def error_resp(message: str, status_code: int = 400):
    return JSONResponse(status_code=status_code, content={"success": False, "message": message, "data": {}})


# -------------------------
# File helpers
# -------------------------
def _save_lifter_file(file: UploadFile, tank_number: str) -> dict:
    file_extension = os.path.splitext(file.filename)[1] if file.filename else ".jpg"
    unique_filename = f"{tank_number}_lifter_weight_{uuid.uuid4().hex}{file_extension}"
    
    # Define subdirectories
    tank_base_dir = os.path.join(IMAGES_ROOT_DIR, tank_number)
    original_dir = os.path.join(tank_base_dir, "original")
    thumbnail_dir = os.path.join(tank_base_dir, "thumbnail")
    
    os.makedirs(original_dir, exist_ok=True)
    os.makedirs(thumbnail_dir, exist_ok=True)

    # Save Original
    dst = os.path.join(original_dir, unique_filename)
    with open(dst, "wb") as buf:
        buf.write(file.file.read())
    
    # Relative path for DB (e.g. "TANK123/original/filename.jpg")
    image_path = f"{tank_number}/original/{unique_filename}"

    thumb_rel = None
    if Image is not None:
        try:
            thumb_name = f"{tank_number}_lifter_weight_{uuid.uuid4().hex}_thumb.jpg"
            thumb_path = os.path.join(thumbnail_dir, thumb_name)
            with Image.open(dst) as img:
                img.thumbnail((200, 200))
                img.convert("RGB").save(thumb_path, format="JPEG")
            
            # Relative path for DB (e.g. "TANK123/thumbnail/filename_thumb.jpg")
            thumb_rel = f"{tank_number}/thumbnail/{thumb_name}"
        except Exception as e:
            logger.warning(f"Warning: thumbnail generation failed for {dst}: {e}")

    return {"image_path": image_path, "thumbnail_path": thumb_rel}


def fetch_pi_next_inspection_date(db: Session, tank_number: str):
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
        if not row:
            return None
        try:
            if hasattr(row, "_mapping"):
                mapping = row._mapping
                return next(iter(mapping.values()), None)
            elif isinstance(row, dict):
                return next(iter(row.values()), None)
            else:
                return row[0]
        except Exception:
            try:
                return list(row.values())[0]
            except Exception:
                return None
    except Exception as exc:
        logger.warning("Could not fetch PI next inspection date for %s: %s", tank_number, exc)
        return None


def generate_report_number(db: Session, inspection_date: datetime) -> str:
    date_str = inspection_date.strftime("%d%m%Y")
    for attempt in range(3):
        try:
            cnt_row = db.execute(
                text("SELECT COUNT(*) AS cnt FROM tank_inspection_details WHERE DATE(inspection_date) = :d"),
                {"d": inspection_date.date()},
            ).fetchone()
            if cnt_row is None:
                count = 0
            else:
                if hasattr(cnt_row, "_mapping"):
                    count = int(cnt_row._mapping.get("cnt", 0))
                elif isinstance(cnt_row, dict):
                    count = int(cnt_row.get("cnt", 0))
                else:
                    count = int(cnt_row[0])
        except Exception:
            count = 0

        next_counter = (count or 0) + 1
        report_number = f"SG-T1-{date_str}-{next_counter:02d}"

        try:
            existing = db.execute(text("SELECT 1 FROM tank_inspection_details WHERE report_number = :rn LIMIT 1"), {"rn": report_number}).fetchone()
            if not existing:
                return report_number
        except Exception:
            return report_number

        logger.warning(f"Report number collision for {report_number}, retrying...")

    raise RuntimeError(f"Unable to generate unique report number after retries for date {date_str}")


def fetch_tank_details(db: Session, tank_number: str):
    result = db.execute(
        text(
            """
            SELECT working_pressure, frame_type, design_temperature, cabinet_type, mfgr, lease
            FROM tank_details
            WHERE tank_number = :tank_number
            LIMIT 1
            """
        ),
        {"tank_number": tank_number},
    ).fetchone()

    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tank details not found for tank_number: {tank_number}",
        )

    try:
        if hasattr(result, "_mapping"):
            mapping = result._mapping
            working_pressure = mapping.get("working_pressure", None)
            frame_type = mapping.get("frame_type", None)
            design_temperature = mapping.get("design_temperature", None)
            cabinet_type = mapping.get("cabinet_type", None)
            mfgr = mapping.get("mfgr", None)
            lease = mapping.get("lease", None)
        else:
            working_pressure = result[0]
            frame_type = result[1]
            design_temperature = result[2]
            cabinet_type = result[3]
            mfgr = result[4]
            lease = result[5]
    except Exception:
        try:
            rowm = dict(result)
            working_pressure = rowm.get("working_pressure")
            frame_type = rowm.get("frame_type")
            design_temperature = rowm.get("design_temperature")
            cabinet_type = rowm.get("cabinet_type")
            mfgr = rowm.get("mfgr")
            lease = rowm.get("lease")
        except Exception:
            working_pressure = frame_type = design_temperature = cabinet_type = mfgr = lease = None

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


# -------------------------
# Pydantic schemas (updated to use tank_id in create/update)
# -------------------------
class TankInspectionCreate(BaseModel):
    created_by: str = Field(..., description="User who created this record (string username/id)")
    tank_id: int = Field(..., description="tank_details.tank_id (client must send tank_id)")
    status_id: Optional[int] = None
    product_id: Optional[int] = None
    inspection_type_id: Optional[int] = None
    location_id: Optional[int] = None
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None  # nullable
    safety_valve_size_id: Optional[int] = None   # nullable
    notes: Optional[str] = None
    operator_id: Optional[int] = None   # manual operator id entered by user (nullable)

    class Config:
        json_schema_extra = {
            "example": {
                "created_by": "string",
                "tank_id": 0,
                "status_id": 0,
                "product_id": 0,
                "inspection_type_id": 0,
                "location_id": 0,
                "safety_valve_brand_id": 0,
                "safety_valve_model_id": 0,
                "safety_valve_size_id": 0,
                "notes": "All checks ok",
                "operator_id": 0
            }
        }

class TankInspectionResponse(BaseModel):
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
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None
    safety_valve_size_id: Optional[int] = None
    notes: Optional[str] = None
    created_by: Optional[str] = None
    operator_id: Optional[int] = None
    emp_id: int     # NOT optional - must be the logged-in user's ID
    ownership: Optional[str] = None
    lifter_weight: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True


class TankInspectionUpdate(BaseModel):
    inspection_date: Optional[datetime] = None
    # client can send tank_id if they want to change which tank this inspection refers to (rare) --
    # if provided, code resolves tank_number from tank_id.
    tank_id: Optional[int] = None
    status_id: Optional[int] = None
    inspection_type_id: Optional[int] = None
    product_id: Optional[int] = None
    location_id: Optional[int] = None
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None      # nullable
    safety_valve_size_id: Optional[int] = None       # nullable

    class Config:
        from_attributes = True


# -------------------------
# Auth helper (kept as before)
# -------------------------
try:
    from app.models.users_model import User
except Exception:
    User = None


def get_current_user(authorization: Optional[str] = Header(None, alias="Authorization"), db: Session = Depends(get_db)):
    if not authorization:
        return None
    auth = authorization.strip()
    token = auth
    if len(auth) >= 6 and auth[:6].lower() == "bearer":
        token_part = auth[6:]
        token = token_part.lstrip(" :\t")
    token = token.strip()
    if not token:
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception:
        return None

    if User is None:
        return payload

    try:
        if "emp_id" in payload and payload["emp_id"] is not None:
            try:
                return db.query(User).filter(User.emp_id == int(payload["emp_id"])).first()
            except Exception:
                return db.query(User).filter(User.emp_id == payload["emp_id"]).first()
        if "email" in payload and payload["email"]:
            return db.query(User).filter(User.email == payload["email"]).first()
        if "sub" in payload and payload["sub"]:
            sub = payload["sub"]
            try:
                return db.query(User).filter((User.email == sub) | (User.emp_id == int(sub))).first()
            except Exception:
                return db.query(User).filter((User.email == sub) | (User.emp_id == sub)).first()
    except Exception:
        return None

    return None


@router.get("/auth/debug-token")
def debug_token(authorization: Optional[str] = Header(None, alias="Authorization")):
    if not authorization:
        return error_resp("No Authorization header", 400)
    token = authorization.strip()
    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM], options={"verify_signature": False})
        return success_resp("Decoded token payload (no signature verification)", payload, 200)
    except Exception as e:
        return error_resp(f"Failed to decode token: {e}", 400)


# -------------------------
# Helper: validate operator exists in operators table
# -------------------------
def operator_exists(db: Session, operator_id: int) -> bool:
    try:
        r = db.execute(text("SELECT 1 FROM operators WHERE operator_id = :op LIMIT 1"), {"op": operator_id}).fetchone()
        return bool(r)
    except Exception:
        return False


# -------------------------
# Masters endpoint (kept)
# -------------------------
@router.get("/masters")
def get_all_tank_inspection_masters():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(DictCursor) as cursor:
            masters = {
                "tank_statuses": ("tank_status", ["status_id", "status_name", "description", "created_at", "updated_at"]),
                "products": ("product_master", ["product_id", "product_name", "description", "created_at", "updated_at"]),
                "inspection_types": ("inspection_type", ["inspection_type_id", "inspection_type_name", "description", "created_at", "updated_at"]),
                "locations": ("location_master", ["location_id", "location_name", "description", "created_at", "updated_at"]),
                "safety_valve_brands": ("safety_valve_brand", ["id", "brand_name", "description", "created_at", "updated_at"]),
                "safety_valve_models": ("safety_valve_model", ["id", "model_name", "description", "created_at", "updated_at"]),
                "safety_valve_sizes": ("safety_valve_size", ["id", "size_label", "description", "created_at", "updated_at"]),
            }

            out_data = {}

            for key, (table, expected_fields) in masters.items():
                try:
                    cursor.execute(f"SELECT * FROM `{table}` LIMIT 100")
                    sample_rows = cursor.fetchall() or []
                except Exception as ex:
                    logger.warning("Failed to fetch table %s: %s", table, ex, exc_info=True)
                    out_data[key] = []
                    continue

                real_cols = list(sample_rows[0].keys()) if sample_rows else []
                if not real_cols:
                    try:
                        cursor.execute(f"SELECT * FROM `{table}` LIMIT 0")
                        real_cols = [d[0] for d in cursor.description] if cursor.description else []
                    except Exception:
                        real_cols = []

                def pick_real_col_for_expected(ef):
                    if ef.endswith("_id"):
                        if ef in real_cols:
                            return ef
                        if "id" in real_cols:
                            return "id"
                        base = ef[:-3]
                        if f"{base}_id" in real_cols:
                            return f"{base}_id"
                        if f"{base}id" in real_cols:
                            return f"{base}id"
                        return None
                    candidates = [ef]
                    if ef.endswith("_name"):
                        candidates.append(ef.replace("_name", "name"))
                        candidates.append(ef.replace("_name", ""))
                    for c in candidates:
                        if c in real_cols:
                            return c
                    for c in real_cols:
                        if c.lower().endswith(ef.split("_")[-1].lower()):
                            return c
                    return None

                chosen_map = {ef: pick_real_col_for_expected(ef) for ef in expected_fields}
                mapped = []
                for r in sample_rows:
                    out_row = {}
                    for ef in expected_fields:
                        real = chosen_map.get(ef)
                        val = None
                        if real and real in r:
                            val = r.get(real)
                        out_row[ef] = val
                    mapped.append(out_row)
                out_data[key] = jsonable_encoder(mapped)

            return success_resp("Master data fetched successfully", out_data, 200)
    except Exception as e:
        logger.error(f"Error fetching masters: {e}", exc_info=True)
        return error_resp("Error fetching master data", 500)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# Simple validator for tank existence (kept)
def validate_tank_exists(db: Session, tank_number: str):
    result = db.execute(
        text("SELECT 1 FROM tank_header WHERE tank_number = :tank_number"),
        {"tank_number": tank_number},
    ).fetchone()
    if not result:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Tank not existing: {tank_number}")


# -------------------------
# Active tanks endpoint
# -------------------------
@router.get("/active-tanks")
def get_active_tanks(db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        rows = db.execute(text("SELECT tank_id, tank_number FROM tank_details WHERE status = 'active'")).mappings().all()
        data = [dict(r) for r in rows]
        return success_resp("Active tanks fetched", {"active_tanks": jsonable_encoder(data)}, 200)
    except Exception as e:
        logger.error(f"Error fetching active tanks: {e}", exc_info=True)
        return error_resp("Error fetching active tanks", 500)


# -------------------------
# Lifter weight thumbnail endpoint
# -------------------------
@router.get("/lifter_weight/{inspection_id}")
def get_lifter_weight_thumbnail(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        # Try to select thumbnail column if it exists
        try:
            row = db.execute(text("SELECT lifter_weight, lifter_weight_thumbnail, tank_number FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        except Exception:
            # Fallback for older schema
            row = db.execute(text("SELECT lifter_weight, tank_number FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()

        if not row:
             return error_resp("No inspection found.", 404)

        # Normalize row
        if hasattr(row, "_mapping"):
            tank_number = row._mapping.get("tank_number")
            rel_path = row._mapping.get("lifter_weight")
            thumb_db = row._mapping.get("lifter_weight_thumbnail") if "lifter_weight_thumbnail" in row._mapping else None
        else:
            # Index based fallback
            rel_path = row[0]
            if len(row) == 3:
                thumb_db = row[1]
                tank_number = row[2]
            else:
                thumb_db = None
                tank_number = row[1]

        if not rel_path:
            return error_resp("No lifter weight photo found for this inspection.", 404)

        # If DB has thumbnail path, return it directly
        if thumb_db:
            return success_resp("Lifter weight thumbnail fetched", {"inspection_id": inspection_id, "thumbnail_path": thumb_db}, 200)

        folder = os.path.dirname(rel_path) if rel_path else ""
        folder_abs = os.path.join(IMAGES_ROOT_DIR, folder)
        if not os.path.isdir(folder_abs):
            old_folder_abs = os.path.join(UPLOAD_DIR, folder)
            if os.path.isdir(old_folder_abs):
                folder_abs = old_folder_abs
        thumb = None
        if os.path.isdir(folder_abs):
            candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")]
            if candidates:
                candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                thumb = f"{folder}/{candidates[0]}"
        return success_resp("Lifter weight thumbnail fetched", {"inspection_id": inspection_id, "thumbnail_path": thumb}, 200)
    except Exception as e:
        logger.error(f"Error fetching lifter weight thumbnail for {inspection_id}: {e}", exc_info=True)
        return error_resp("Internal server error", 500)


# -------------------------
# Create Tank Inspection (flat payload with master ids)
# -------------------------
@router.post("/create/tank_inspection", status_code=status.HTTP_201_CREATED)
def create_tank_inspection(
    payload: TankInspectionCreate,
    db: Session = Depends(get_db),
    current_user: Optional[dict] = Depends(get_current_user),
):
    try:
        # --- Resolve tank_number from payload.tank_id ---
        try:
            tn_row = db.execute(
                text("SELECT tank_number FROM tank_details WHERE tank_id = :tid LIMIT 1"),
                {"tid": payload.tank_id},
            ).fetchone()
        except Exception as e:
            logger.error("DB error resolving tank_number: %s", e, exc_info=True)
            return error_resp(f"Tank not found for id: {payload.tank_id}", 404)

        if not tn_row:
            return error_resp(f"Tank not found for id: {payload.tank_id}", 404)

        # Handle row mapping safely
        if hasattr(tn_row, "_mapping"):
            tank_number = tn_row._mapping.get("tank_number")
        elif isinstance(tn_row, dict):
            tank_number = tn_row.get("tank_number")
        else:
            tank_number = tn_row[0]

        # --- Helper: Strictly check if value is a valid ID ---
        def is_valid_id(val):
            if val is None: return False
            if isinstance(val, int) and val > 0: return True
            if isinstance(val, str) and val.isdigit() and int(val) > 0: return True
            return False

        # --- Validate master ids (Only if provided) ---
        master_checks = [
            ("tank_status", payload.status_id, "status_id"),
            ("product_master", payload.product_id, "product_id"),
            ("inspection_type", payload.inspection_type_id, "inspection_type_id"),
            ("location_master", payload.location_id, "location_id"),
            # Safety valves (Optional)
            ("safety_valve_brand", payload.safety_valve_brand_id, "safety_valve_brand_id"),
            ("safety_valve_model", payload.safety_valve_model_id, "safety_valve_model_id"),
            ("safety_valve_size", payload.safety_valve_size_id, "safety_valve_size_id"),
        ]

        for table, val, name in master_checks:
            # Skip validation if value is empty/null/zero
            if not is_valid_id(val):
                continue
            
            # Check column variations
            cols_to_try = ["id", name]
            if name and "_id" in name:
                base = name.replace("_id", "")
                cols_to_try.append(base + "id")
                cols_to_try.append(base)

            r = None
            for col in cols_to_try:
                try:
                    r = db.execute(text(f"SELECT 1 FROM `{table}` WHERE `{col}` = :id LIMIT 1"), {"id": val}).fetchone()
                except Exception:
                    r = None
                if r:
                    break

            if not r:
                return error_resp(f"Invalid {name}: {val}", 400)

        # --- Prepare Inspection Data ---
        tank_details = fetch_tank_details(db, tank_number)
        inspection_date = datetime.now()
        
        # Resolve Emp ID
        emp_id_val = None
        if current_user:
            for key in ("emp_id", "id", "user_id", "sub"):
                val = None
                if isinstance(current_user, dict):
                    val = current_user.get(key)
                else:
                    val = getattr(current_user, key, None)
                
                if val:
                    emp_id_val = val
                    break

        # Duplicate Check
        existing = db.execute(
            text("SELECT inspection_id FROM tank_inspection_details WHERE tank_number = :tn AND DATE(inspection_date) = :d AND inspection_type_id = :itype LIMIT 1"),
            {"tn": tank_number, "d": inspection_date.date(), "itype": payload.inspection_type_id},
        ).fetchone()
        if existing:
            return error_resp("Inspection already exists", 400)

        # Generate Reports
        report_number = generate_report_number(db, inspection_date)
        pi_next_date = fetch_pi_next_inspection_date(db, tank_number)

        # Ownership Logic
        lease_val = str(tank_details.get("lease", "")).lower()
        ownership_val = "leased" if lease_val in ("yes", "y", "1") else "owned"

        # Sanitize Safety Valve IDs for Insert (Ensure None if invalid)
        svb = payload.safety_valve_brand_id if is_valid_id(payload.safety_valve_brand_id) else None
        svm = payload.safety_valve_model_id if is_valid_id(payload.safety_valve_model_id) else None
        svs = payload.safety_valve_size_id if is_valid_id(payload.safety_valve_size_id) else None

        # --- INSERT ---
        try:
            db.execute(
                text("""
                    INSERT INTO tank_inspection_details
                    (inspection_date, report_number, tank_number, tank_id, status_id, product_id, inspection_type_id, location_id,
                     working_pressure, frame_type, design_temperature, cabinet_type, mfgr, pi_next_inspection_date,
                     safety_valve_brand_id, safety_valve_model_id, safety_valve_size_id, notes, created_by, updated_by,
                     operator_id, emp_id, ownership, created_at, updated_at)
                    VALUES
                    (:inspection_date, :report_number, :tank_number, :tank_id, :status_id, :product_id, :inspection_type_id, :location_id,
                     :working_pressure, :frame_type, :design_temperature, :cabinet_type, :mfgr, :pi_next_inspection_date,
                     :svb, :svm, :svs, :notes, :created_by, :updated_by,
                     :operator_id, :emp_id, :ownership, NOW(), NOW())
                """),
                {
                    "inspection_date": inspection_date,
                    "report_number": report_number,
                    "tank_number": tank_number,
                    "tank_id": payload.tank_id,
                    "status_id": payload.status_id if is_valid_id(payload.status_id) else None,
                    "product_id": payload.product_id if is_valid_id(payload.product_id) else None,
                    "inspection_type_id": payload.inspection_type_id if is_valid_id(payload.inspection_type_id) else None,
                    "location_id": payload.location_id if is_valid_id(payload.location_id) else None,
                    "working_pressure": tank_details.get("working_pressure"),
                    "frame_type": tank_details.get("frame_type"),
                    "design_temperature": tank_details.get("design_temperature"),
                    "cabinet_type": tank_details.get("cabinet_type"),
                    "mfgr": tank_details.get("mfgr"),
                    "pi_next_inspection_date": pi_next_date,
                    "svb": svb, "svm": svm, "svs": svs,
                    "notes": payload.notes,
                    "created_by": payload.created_by,
                    "updated_by": payload.created_by,
                    "operator_id": payload.operator_id,
                    "emp_id": emp_id_val,
                    "ownership": ownership_val,
                },
            )
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error("Failed to create tank inspection record: %s", e, exc_info=True)
            return error_resp(f"Internal server error: {e}", 500)

        # --- Return Created Record ---
        new_row = db.execute(text("SELECT * FROM tank_inspection_details WHERE report_number = :rn"), {"rn": report_number}).fetchone()
        
        # Convert row to dict safely
        if hasattr(new_row, "_mapping"):
            out = dict(new_row._mapping)
        elif isinstance(new_row, dict):
            out = new_row
        else:
            out = dict(zip(new_row.keys(), new_row))

        return success_resp("Inspection created successfully", out, 201)

    except Exception as e:
        logger.error(f"Error creating tank inspection: {e}", exc_info=True)
        return error_resp(f"Internal server error: {e}", 500)


# -------------------------
# Update tank_inspection_details (PUT)
# -------------------------
class TankInspectionUpdateModel(BaseModel):
    inspection_date: Optional[datetime] = None
    tank_id: Optional[int] = None
    status_id: Optional[int] = None
    inspection_type_id: Optional[int] = None
    product_id: Optional[int] = None
    location_id: Optional[int] = None
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None      # nullable
    safety_valve_size_id: Optional[int] = None       # nullable

    class Config:
        from_attributes = True


@router.put("/update/tank_inspection_details/{inspection_id}")
def update_tank_inspection_details(
    inspection_id: int, 
    payload: TankInspectionUpdateModel, 
    db: Session = Depends(get_db), 
    current_user: Optional[dict] = Depends(get_current_user)
):
    try:
        # 1. Check if inspection exists
        row = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp("Inspection not found", 404)

        params = {"id": inspection_id}
        updates = []

        # Helper: Strictly check if value is a valid ID (int > 0)
        def is_valid_id(val):
            if val is None: return False
            if isinstance(val, int) and val > 0: return True
            if isinstance(val, str) and val.isdigit() and int(val) > 0: return True
            return False

        # Helper: Get set fields safely (Pydantic v1/v2 compat)
        try:
            # Try Pydantic v2
            update_data = payload.model_dump(exclude_unset=True)
        except AttributeError:
            # Fallback to Pydantic v1
            update_data = payload.dict(exclude_unset=True)

        # --- Handle Special Fields (operator_id, emp_id, tank_id) ---
        
        # Operator ID (Optional)
        if "operator_id" in update_data:
            op_id = update_data["operator_id"]
            # Treat 0 as None if needed, or just pass it. Assuming 0 means "no operator" -> None
            if op_id == 0:
                op_id = None
            updates.append("operator_id = :operator_id")
            params["operator_id"] = op_id

        # Emp ID (Auto-resolve)
        emp_id_val = None
        if current_user:
            for k in ["emp_id", "id", "user_id", "sub"]:
                val = None
                if isinstance(current_user, dict):
                    val = current_user.get(k)
                else:
                    val = getattr(current_user, k, None)
                
                if val:
                    try:
                        emp_id_val = int(val)
                        break
                    except:
                        emp_id_val = val
                        break
        if emp_id_val:
            updates.append("emp_id = :emp_id")
            params["emp_id"] = emp_id_val

        # Tank ID (Resolve Number)
        if "tank_id" in update_data and update_data["tank_id"] is not None:
            tid = update_data["tank_id"]
            tn_row = db.execute(text("SELECT tank_number FROM tank_details WHERE tank_id = :tid LIMIT 1"), {"tid": tid}).fetchone()
            if not tn_row:
                return error_resp(f"Tank not found for id: {tid}", 404)
            
            tank_num = tn_row._mapping.get("tank_number") if hasattr(tn_row, "_mapping") else tn_row[0]
            updates.append("tank_id = :tank_id")
            updates.append("tank_number = :tank_number")
            params["tank_id"] = tid
            params["tank_number"] = tank_num

        # --- Handle Standard Fields ---
        fields_to_update = [
            "inspection_date", "status_id", "inspection_type_id", "product_id", "location_id",
            "working_pressure", "frame_type", "design_temperature", "cabinet_type", "mfgr",
            "notes", "ownership", "safety_valve_brand_id", "safety_valve_model_id", "safety_valve_size_id"
        ]

        for field in fields_to_update:
            if field in update_data:
                val = update_data[field]
                
                # Special Logic for Safety Valve Fields: Force invalid/empty to None
                if field in ["safety_valve_model_id", "safety_valve_size_id"]:
                    if not is_valid_id(val):
                        val = None
                
                # Special Logic for other IDs: Treat 0 as None if desired (based on user request)
                if field in ["status_id", "product_id", "inspection_type_id", "location_id", "safety_valve_brand_id"]:
                     if val == 0:
                         val = None

                updates.append(f"{field} = :{field}")
                params[field] = val

        # --- Execute Update & Validate ---
        if updates:
            sql = f"UPDATE tank_inspection_details SET {', '.join(updates)}, updated_at = NOW() WHERE inspection_id = :id"
            
            try:
                # Validation: Check if provided IDs exist (Only if they are NOT None)
                
                # Check Model
                if "safety_valve_model_id" in params:
                    mid = params["safety_valve_model_id"]
                    if mid is not None: # Strict None check
                        exists = db.execute(text("SELECT 1 FROM safety_valve_model WHERE id = :id LIMIT 1"), {"id": mid}).fetchone()
                        if not exists:
                            return error_resp(f"Invalid safety_valve_model_id: {mid}", 400)

                # Check Size
                if "safety_valve_size_id" in params:
                    sid = params["safety_valve_size_id"]
                    if sid is not None: # Strict None check
                        exists = db.execute(text("SELECT 1 FROM safety_valve_size WHERE id = :id LIMIT 1"), {"id": sid}).fetchone()
                        if not exists:
                            return error_resp(f"Invalid safety_valve_size_id: {sid}", 400)

                # Run Update
                db.execute(text(sql), params)
                db.commit()
                
            except Exception as e:
                db.rollback()
                logger.error(f"DB Error during update: {e}", exc_info=True)
                raise e

        return success_resp("Inspection details updated", {"inspection_id": inspection_id}, 200)

    except Exception as e:
        logger.error(f"Error updating tank inspection details {inspection_id}: {e}", exc_info=True)
        return error_resp("Error updating inspection details", 500)


# File: /mnt/data/tank_inspection_router.py
# Replace the existing @router.get("/review/{inspection_id}") handler with this complete function.


@router.get("/review/{inspection_id}")
def get_inspection_review(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        # ---------------------------------------------------------
        # 1. Fetch Inspection Details with JOINS
        # ---------------------------------------------------------
        # FIXED: Changed 'LEFT JOIN tanks' to 'LEFT JOIN tank_details'
        query_str = """
            SELECT 
                ti.*,
                t.tank_number,
                ps.status_name AS status_val,
                pl.location_name AS location_val,
                pit.inspection_type_name AS inspection_type_val,
                psvb.brand_name AS safety_valve_brand_val,
                pm.product_name AS product_val
            FROM tank_inspection_details ti
            LEFT JOIN tank_details t ON ti.tank_id = t.tank_id
            LEFT JOIN tank_status ps ON ti.status_id = ps.status_id
            LEFT JOIN location_master pl ON ti.location_id = pl.location_id
            LEFT JOIN inspection_type pit ON ti.inspection_type_id = pit.inspection_type_id
            LEFT JOIN safety_valve_brand psvb ON ti.safety_valve_brand_id = psvb.id
            LEFT JOIN product_master pm ON ti.product_id = pm.product_id
            WHERE ti.inspection_id = :id
        """
        
        inspection = db.execute(text(query_str), {"id": inspection_id}).fetchone()

        if not inspection:
            return error_resp("Inspection not found", 404)

        # Normalize inspection row into a dict
        try:
            if hasattr(inspection, "_mapping"):
                insp = dict(inspection._mapping)
            elif isinstance(inspection, dict):
                insp = inspection
            else:
                insp = dict((k, v) for k, v in zip(inspection.keys(), inspection))
        except Exception:
            insp = jsonable_encoder(inspection)

        # --- DATA MAPPING: Overwrite IDs with Values ---
        insp["status"] = insp.get("status_val") or insp.get("status_id")
        insp["location"] = insp.get("location_val") or insp.get("location_id")
        insp["inspection_type"] = insp.get("inspection_type_val") or insp.get("inspection_type_id")
        insp["safety_valve_brand"] = insp.get("safety_valve_brand_val") or insp.get("safety_valve_brand_id")
        insp["product"] = insp.get("product_val") or insp.get("product_id")

        # Cleanup helper columns
        for key in ["status_val", "location_val", "inspection_type_val", "safety_valve_brand_val", "product_val"]:
            insp.pop(key, None)

        # Remove DB timestamps
        insp.pop("created_at", None)
        insp.pop("updated_at", None)

        # ---------------------------------------------------------
        # 2. Fetch Images (Simplified Direct DB Query)
        # ---------------------------------------------------------
        images_list = []
        try:
            # Assumes table is 'tank_images' and column is 'image_path'
            imgs_query = text("SELECT image_path, image_type, thumbnail_path FROM tank_images WHERE inspection_id = :iid")
            imgs_rows = db.execute(imgs_query, {"iid": inspection_id}).fetchall()
            
            for row in imgs_rows:
                # Convert row to dict safely
                r_dict = dict(row._mapping) if hasattr(row, "_mapping") else dict(zip(row.keys(), row))
                
                # Filter out lifter weight if needed (as it's usually separate)
                if str(r_dict.get("image_type")).lower() == "lifter_weight":
                    continue

                images_list.append({
                    "image_type": r_dict.get("image_type"),
                    "image_path": r_dict.get("image_path"),
                    "thumbnail_path": r_dict.get("thumbnail_path")
                })
        except Exception as e:
            logger.error(f"Error fetching images: {e}")
            images_list = []

        # ---------------------------------------------------------
        # 3. Lifter Weight Thumbnail Logic
        # ---------------------------------------------------------
        lifter_thumb = None
        # (Your existing file logic was commented out in previous versions, setting to None for now)
        insp["lifter_weight_thumbnail"] = lifter_thumb 

        # ---------------------------------------------------------
        # 4. Fetch Checklist & To-Do List
        # ---------------------------------------------------------
        checklist_out = []
        try:
            # Re-fetch inspection status map for checklist items
            inspection_status_rows = db.execute(text("SELECT status_id, status_name FROM tank_status")).fetchall()
            inspection_status_map = {row.status_id: row.status_name for row in inspection_status_rows}
            
            rows = db.execute(text("SELECT * FROM inspection_checklist WHERE inspection_id = :iid ORDER BY id ASC"), {"iid": inspection_id}).fetchall()
            
            for r in rows:
                rr = dict(r._mapping) if hasattr(r, "_mapping") else dict(zip(r.keys(), r))
                
                checklist_out.append({
                    "id": rr.get("id"),
                    "job_id": rr.get("job_id"),
                    "job_name": rr.get("job_name"),
                    "sub_job_name": rr.get("sub_job_name") or rr.get("sub_job_description"),
                    "sub_job_id": rr.get("sub_job_id"),
                    "sn": rr.get("sn"),
                    "status_id": rr.get("status_id"),
                    "status": rr.get("status"), 
                    "comment": rr.get("comment"),
                })
        except Exception:
            checklist_out = []

        # --- Grouping Logic ---
        grouped_sections = []
        try:
            from collections import OrderedDict
            job_groups = OrderedDict()
            
            for it in checklist_out:
                job_key = it.get("job_id") if it.get("job_id") else it.get("job_name")
                
                if job_key not in job_groups:
                    job_groups[job_key] = {
                        "job_id": it.get("job_id"),
                        "title": it.get("job_name"),
                        "status_name": "OK", 
                        "items": []
                    }
                
                # Determine Item Status Name
                item_status = inspection_status_map.get(it.get("status_id")) or it.get("status") or "OK"

                job_groups[job_key]["items"].append({
                    "sn": it.get("sn"),
                    "title": it.get("sub_job_name"),
                    "comments": it.get("comment"),
                    "sub_job_id": it.get("sub_job_id"),
                    "status_name": item_status 
                })

            grouped_sections = list(job_groups.values())
            failed_items = [item for item in checklist_out if item.get("status_id") == 2 or item.get("flagged") == 1]

        except Exception as e:
            logger.error(f"Grouping Error: {e}")
            grouped_sections = []
        

        # ---------------------------------------------------------
        # 5. Build Response
        # ---------------------------------------------------------
        resp = {
            "inspection": insp,
            "images": images_list, 
            "inspection_checklist": grouped_sections,
            "to_do_list": failed_items
        }
        
        return success_resp("Inspection review fetched", resp, 200)

    except Exception as e:
        # DEBUG MODE: This will show you the exact SQL error in Postman instead of "Error fetching..."
        logger.error(f"Error fetching review for {inspection_id}: {e}", exc_info=True)
        return error_resp(f"CRASH REPORT: {str(e)}", 500)

@router.delete("/review/{inspection_id}")
def delete_inspection_review(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        row = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp("Inspection not found", 404)
        try:
            try:
                if hasattr(row, "_mapping"):
                    insp = dict(row._mapping)
                elif isinstance(row, dict):
                    insp = row
                else:
                    insp = dict((k, v) for k, v in row)
            except Exception:
                insp = jsonable_encoder(row)

            # Delete related checklist and to-do items (will cascade delete due to FK constraints)
            try:
                db.execute(text("DELETE FROM inspection_checklist WHERE inspection_id = :iid"), {"iid": inspection_id})
                db.execute(text("DELETE FROM to_do_list WHERE inspection_id = :iid"), {"iid": inspection_id})
            except Exception:
                db.rollback()
            db.execute(text("DELETE FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id})
            db.commit()
            return success_resp("Inspection and related checklist/to-do entries deleted", {"inspection_id": inspection_id}, 200)
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting review for {inspection_id}: {e}", exc_info=True)
            return error_resp(str(e), 500)
    except Exception as e:
        logger.error(f"Unexpected error deleting review for {inspection_id}: {e}", exc_info=True)
        return error_resp("Internal server error", 500)


# -------------------------
# Upload lifter weight (create/replace) endpoint
# -------------------------
@router.post("/{inspection_id}/lifter_weight", status_code=200)
def upload_lifter_weight(inspection_id: int, file: UploadFile = File(...), db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        # Try to fetch thumbnail column as well
        try:
            row = db.execute(text("SELECT inspection_id, tank_number, lifter_weight, lifter_weight_thumbnail FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        except Exception:
            row = db.execute(text("SELECT inspection_id, tank_number, lifter_weight FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()

        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)
        
        # Normalize row access
        thumb_rel = None
        if hasattr(row, "_mapping"):
            tank_number = row._mapping.get("tank_number")
            old_rel = row._mapping.get("lifter_weight")
            thumb_rel = row._mapping.get("lifter_weight_thumbnail") if "lifter_weight_thumbnail" in row._mapping else None
        else:
            try:
                old_rel = row[2]
                tank_number = row[1]
                if len(row) > 3:
                    thumb_rel = row[3]
            except Exception:
                old_rel = None
                tank_number = None

        if not file.content_type or not file.content_type.startswith("image/"):
            return error_resp("File must be an image", 400)

        saved = _save_lifter_file(file, tank_number)
        rel_path = saved["image_path"]
        thumb_path = saved.get("thumbnail_path")

        # Cleanup old files
        try:
            if old_rel:
                old_abs = os.path.join(IMAGES_ROOT_DIR, *old_rel.split("/"))
                if os.path.exists(old_abs):
                    try:
                        os.remove(old_abs)
                    except Exception:
                        logger.debug("Could not remove old lifter file: %s", old_abs, exc_info=True)
                
                # Cleanup old thumbnail (Explicit Path)
                if thumb_rel:
                    try:
                        thumb_abs = os.path.join(IMAGES_ROOT_DIR, *thumb_rel.split("/"))
                        if os.path.exists(thumb_abs):
                            os.remove(thumb_abs)
                    except Exception:
                        pass
                
                # Cleanup inferred thumbnail (if explicit path was missing but file exists in new structure)
                # old_rel might be "TANK/original/file.jpg" -> we check "TANK/thumbnail/file_thumb.jpg"
                try:
                    old_dir_name = os.path.dirname(old_abs) # .../original
                    old_file_name = os.path.basename(old_abs) # file.jpg
                    
                    # Check if we are in an 'original' folder
                    if os.path.basename(old_dir_name) == "originals":
                        base_dir = os.path.dirname(old_dir_name) # .../TANK
                        thumb_dir = os.path.join(base_dir, "thumbnails")
                        
                        # Construct expected thumbnail name
                        # Original: {tank}_{uuid}.jpg -> Thumb: {tank}_{uuid}_thumb.jpg
                        name_part, ext_part = os.path.splitext(old_file_name)
                        expected_thumb_name = f"{name_part}_thumb.jpg"
                        
                        expected_thumb_path = os.path.join(thumb_dir, expected_thumb_name)
                        if os.path.exists(expected_thumb_path):
                            try:
                                os.remove(expected_thumb_path)
                            except Exception:
                                pass
                except Exception:
                    pass

                # Legacy Cleanup (Same folder - for very old files)
                try:
                    old_base = os.path.splitext(os.path.basename(old_abs))[0]
                    folder = os.path.dirname(old_abs)
                    if os.path.isdir(folder):
                        for fn in os.listdir(folder):
                            if old_base in fn and "thumb" in fn:
                                try:
                                    os.remove(os.path.join(folder, fn))
                                except Exception:
                                    pass
                except Exception:
                    pass
        except Exception:
            logger.debug("Error while cleaning old lifter files for inspection %s", inspection_id, exc_info=True)

        # --- DB UPDATE (FIXED WITH THUMBNAIL) ---
        try:
            db.execute(text("""
                UPDATE tank_inspection_details 
                SET lifter_weight = :lp, 
                    lifter_weight_thumbnail = :thumb, 
                    updated_at = NOW() 
                WHERE inspection_id = :id
            """), {
                "lp": rel_path, 
                "thumb": thumb_path,
                "id": inspection_id
            })
            db.commit()
        except Exception as e_main:
            db.rollback()
            logger.warning(f"Failed to update with thumbnail column, trying without: {e_main}")
            # Fallback: maybe lifter_weight_thumbnail column doesn't exist?
            try:
                db.execute(text("""
                    UPDATE tank_inspection_details 
                    SET lifter_weight = :lp, 
                        updated_at = NOW() 
                    WHERE inspection_id = :id
                """), {
                    "lp": rel_path, 
                    "id": inspection_id
                })
                db.commit()
            except Exception as e_fallback:
                db.rollback()
                logger.error("Failed to update lifter_weight column (fallback also failed)", exc_info=True)
                return error_resp(f"Failed to save lifter weight path to DB: {e_fallback}", 500)

        return success_resp("Lifter weight photo uploaded", {"inspection_id": inspection_id, "lifter_weight": rel_path, "thumbnail": thumb_path}, 200)

    except Exception as e:
        logger.error(f"Error uploading lifter weight for inspection {inspection_id}: {e}", exc_info=True)
        return error_resp("Error uploading lifter weight", 500)

@router.delete("/delete/inspection_details/{inspection_id}")
def delete_inspection_details(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        row = db.execute(text("SELECT inspection_id FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)
        try:
            db.execute(text("DELETE FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id})
            db.commit()
            return success_resp("Inspection deleted", {"inspection_id": inspection_id}, 200)
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting inspection {inspection_id}: {e}", exc_info=True)
            return error_resp("Error deleting inspection", 500)
    except Exception as e:
        logger.error(f"Unexpected error deleting inspection {inspection_id}: {e}", exc_info=True)
        return error_resp("Internal server error", 500)


# -------------------------
# Tank details endpoint (keeps unfilled detection logic unchanged)
# -------------------------
@router.get("/tank-details/{tank_id}")
def get_tank_details(tank_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        tn_row = db.execute(
            text("SELECT tank_number FROM tank_details WHERE tank_id = :tid LIMIT 1"),
            {"tid": tank_id}
        ).fetchone()

        if not tn_row:
            return error_resp(f"Tank not found for id: {tank_id}", 404)

        if hasattr(tn_row, "_mapping"):
            tank_number = tn_row._mapping.get("tank_number")
        elif isinstance(tn_row, dict):
            tank_number = tn_row.get("tank_number")
        else:
            tank_number = tn_row[0] if len(tn_row) > 0 else None

        if not tank_number:
            return error_resp(f"Tank number missing for id: {tank_id}", 404)

        row = db.execute(
            text("""
                SELECT working_pressure, design_temperature, frame_type, cabinet_type, mfgr, lease
                FROM tank_details
                WHERE tank_number = :tank_number
                LIMIT 1
            """),
            {"tank_number": tank_number},
        ).fetchone()

        if not row:
            return error_resp(f"Tank not found: {tank_number}", 404)

        if hasattr(row, "_mapping"):
            row_map = dict(row._mapping)
        elif isinstance(row, dict):
            row_map = row
        else:
            row_map = {
                "working_pressure": row[0] if len(row) > 0 else None,
                "design_temperature": row[1] if len(row) > 1 else None,
                "frame_type": row[2] if len(row) > 2 else None,
                "cabinet_type": row[3] if len(row) > 3 else None,
                "mfgr": row[4] if len(row) > 4 else None,
                "lease": row[5] if len(row) > 5 else None,
            }

        working_pressure = row_map.get("working_pressure")
        design_temperature = row_map.get("design_temperature")
        frame_type = row_map.get("frame_type")
        cabinet_type = row_map.get("cabinet_type")
        mfgr = row_map.get("mfgr")
        lease_val = row_map.get("lease")

        ownership_val: Optional[str] = None
        try:
            if isinstance(lease_val, str):
                l = lease_val.strip().lower()
                if l in ("no", "n", "0", ""):
                    ownership_val = "owned"
                elif l in ("yes", "y", "1"):
                    ownership_val = "leased"
            elif lease_val in (0, "0"):
                ownership_val = "owned"
            elif lease_val in (1, "1"):
                ownership_val = "leased"
        except Exception:
            ownership_val = None

        try:
            pi_next = fetch_pi_next_inspection_date(db, tank_number)
        except Exception:
            pi_next = None

        def conv_decimal(v):
            try:
                return float(v) if isinstance(v, Decimal) else v
            except Exception:
                return v

        data = {
            "tank_id": tank_id,
            "tank_number": tank_number,
            "working_pressure": conv_decimal(working_pressure),
            "design_temperature": conv_decimal(design_temperature),
            "frame_type": frame_type,
            "cabinet_type": cabinet_type,
            "mfgr": mfgr,
            "ownership": ownership_val,
            "pi_next_inspection_date": (pi_next.isoformat() if hasattr(pi_next, "isoformat") else None),
        }

        # determine unfilled inspection with same logic as before
        inspection_id = None
        try:
            cols_q = db.execute(
                text("""
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'tank_inspection_details'
                      AND IS_NULLABLE = 'YES'
                """)
            ).fetchall()

            nullable_cols: List[str] = []
            for c in cols_q:
                if hasattr(c, "_mapping"):
                    name = c._mapping.get("COLUMN_NAME")
                elif isinstance(c, dict):
                    name = c.get("COLUMN_NAME")
                else:
                    name = c[0] if len(c) > 0 else None
                if name:
                    if name.lower() not in ("inspection_id", "tank_number", "created_at", "updated_at"):
                        nullable_cols.append(name)

            if nullable_cols:
                null_conds = " OR ".join([f"`{col}` IS NULL" for col in nullable_cols])
                insp_sql = text(f"""
                    SELECT inspection_id
                    FROM tank_inspection_details
                    WHERE tank_number = :tn
                      AND ({null_conds})
                    ORDER BY inspection_id DESC
                    LIMIT 1
                """)
                insp_row = db.execute(insp_sql, {"tn": tank_number}).fetchone()
                if insp_row:
                    if hasattr(insp_row, "_mapping"):
                        inspection_id = insp_row._mapping.get("inspection_id")
                    elif isinstance(insp_row, dict):
                        inspection_id = insp_row.get("inspection_id")
                    else:
                        inspection_id = insp_row[0]
            else:
                inspection_id = None

        except Exception as e:
            logger.warning("Failed to detect unfilled inspection via information_schema: %s", e, exc_info=True)
            inspection_id = None

        data["inspection_id"] = inspection_id

        return success_resp("Tank details fetched", data, 200)

    except Exception as e:
        logger.error(f"Error fetching tank details for tank_id={tank_id}: {e}", exc_info=True)
        return error_resp("Error fetching tank details", 500)


# -------------------------
# New: fetch inspection by id (returns requested fields)
# -------------------------
@router.get("/get/inspection/{inspection_id}")
def get_inspection_by_id(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    """
    Fetch inspection record by inspection_id with all required fields.
    """
    try:
        row = db.execute(
            text("""
                SELECT 
                    inspection_id,
                    tank_id,
                    tank_number,
                    report_number,
                    inspection_date,
                    status_id,
                    product_id,
                    inspection_type_id,
                    location_id,
                    working_pressure,
                    design_temperature,
                    frame_type,
                    cabinet_type,
                    mfgr,
                    pi_next_inspection_date,
                    safety_valve_brand_id,
                    safety_valve_model_id,
                    safety_valve_size_id,
                    notes,
                    created_by,
                    operator_id,
                    emp_id,
                    ownership,
                    lifter_weight
                FROM tank_inspection_details
                WHERE inspection_id = :id
                LIMIT 1
            """),
            {"id": inspection_id}
        ).fetchone()

        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)

        # Convert row → dict safely
        if hasattr(row, "_mapping"):
            out = dict(row._mapping)
        elif isinstance(row, dict):
            out = row
        else:
            try:
                out = dict(row)
            except:
                out = jsonable_encoder(row)

        # Keep ids (status_id/product_id/etc.) and return tank_id instead of tank_number
        try:
            # Ensure we do not return tank_number; prefer tank_id
            out.pop("tank_number", None)
        except Exception:
            pass

        return success_resp("Inspection fetched successfully", out, 200)

    except Exception as e:
        logger.error(f"Error fetching inspection {inspection_id}: {e}", exc_info=True)
        return error_resp("Internal server error", 500)


# -------------------------
# Delete lifter weight endpoint (keeps same semantics)
# -------------------------
@router.delete("/{inspection_id}/lifter_weight", status_code=200)
def delete_lifter_weight(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        # Try to fetch thumbnail column as well
        try:
            row = db.execute(text("SELECT inspection_id, tank_number, lifter_weight, lifter_weight_thumbnail FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        except Exception:
            row = db.execute(text("SELECT inspection_id, tank_number, lifter_weight FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()

        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)

        thumb_rel = None
        if hasattr(row, "_mapping"):
            rel = row._mapping.get("lifter_weight")
            thumb_rel = row._mapping.get("lifter_weight_thumbnail") if "lifter_weight_thumbnail" in row._mapping else None
        else:
            try:
                rel = row[2]
                if len(row) > 3:
                    thumb_rel = row[3]
            except Exception:
                rel = None

        if not rel:
            return error_resp("No lifter weight image present for this inspection", 404)

        # Delete Original
        try:
            abs_path = os.path.join(IMAGES_ROOT_DIR, *rel.split("/"))
            if os.path.exists(abs_path):
                try:
                    os.remove(abs_path)
                except Exception:
                    logger.debug("Could not remove lifter file %s", abs_path, exc_info=True)
        except Exception:
             pass

        # Delete Thumbnail (Explicit Path)
        if thumb_rel:
            try:
                thumb_abs = os.path.join(IMAGES_ROOT_DIR, *thumb_rel.split("/"))
                if os.path.exists(thumb_abs):
                    os.remove(thumb_abs)
            except Exception:
                pass
        
        # Legacy Thumbnail Cleanup (Same folder)
        try:
            folder = os.path.dirname(abs_path)
            base_no_ext = os.path.splitext(os.path.basename(abs_path))[0]
            if os.path.isdir(folder):
                for fn in os.listdir(folder):
                    if base_no_ext in fn and "thumb" in fn:
                        try:
                            os.remove(os.path.join(folder, fn))
                        except Exception:
                            pass
        except Exception:
            pass

        try:
            # Try to nullify both columns
            try:
                db.execute(text("UPDATE tank_inspection_details SET lifter_weight = NULL, lifter_weight_thumbnail = NULL, updated_at = NOW() WHERE inspection_id = :id"), {"id": inspection_id})
            except Exception:
                db.execute(text("UPDATE tank_inspection_details SET lifter_weight = NULL, updated_at = NOW() WHERE inspection_id = :id"), {"id": inspection_id})
            
            db.commit()
        except Exception:
            db.rollback()
            logger.error("Failed to clear lifter_weight DB column", exc_info=True)
            return error_resp("Failed to remove lifter weight reference from DB", 500)

        return success_resp("Lifter weight image deleted", {"inspection_id": inspection_id}, 200)
    except Exception as e:
        logger.error(f"Error deleting lifter weight for inspection {inspection_id}: {e}", exc_info=True)
        return error_resp("Error deleting lifter weight", 500)
    
@router.put("/{inspection_id}/lifter_weight")
def update_lifter_weight(inspection_id: int, file: UploadFile = File(...), db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    """
    Update (Replace) the lifter weight image for an inspection.
    This logic calls the existing upload function but exposes it via PUT
    for semantic correctness (Replace existing resource).
    """
    # We reuse the logic from the existing POST function
    return upload_lifter_weight(inspection_id, file, db)

# ----------------------------
# SUBMIT INSPECTION (Finalize)
# ----------------------------
@router.post("/submit/{inspection_id}")
def submit_inspection(inspection_id: int, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    """
    Finalize the inspection.
    First validates that all required data is complete (no null values, all images present, to_do_list empty).
    Only submits if validation passes.
    Sets the status_id to 4 (Completed) and updates the timestamp.
    """
    try:
        # 1. Verify Inspection Exists
        row = db.execute(text("SELECT inspection_id FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)

        # 2. Run Validation Check (same logic as validation endpoint)
        issues = {"inspection": [], "checklist": [], "to_do_list": [], "images": []}
        
        # 2a. Check inspection row
        try:
            insp_row = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id LIMIT 1"), {"id": inspection_id}).fetchone()
            if hasattr(insp_row, "_mapping"):
                insp = dict(insp_row._mapping)
            elif isinstance(insp_row, dict):
                insp = insp_row
            else:
                try:
                    insp = dict(zip(insp_row.keys(), insp_row))
                except Exception:
                    insp = {}

            required_inspection_fields = [
                "tank_id", "tank_number", "report_number", "inspection_date",
                "status_id", "product_id", "inspection_type_id", "location_id",
            ]

            for f in required_inspection_fields:
                v = insp.get(f)
                if v is None or (isinstance(v, str) and v.strip() == ""):
                    issues["inspection"].append({"field": f, "reason": "null or empty"})
                else:
                    if isinstance(v, (int, float)) and int(v) == 0:
                        issues["inspection"].append({"field": f, "reason": "zero or invalid"})

            # Validate PI next inspection date
            pi_keys = ["pi_next_inspection_date", "pi_next_insp_date", "next_insp_date", "pi_nextinsp_date"]
            pi_found = False
            for k in pi_keys:
                v = insp.get(k)
                if v is not None and not (isinstance(v, str) and v.strip() == ""):
                    pi_found = True
                    break
            if not pi_found:
                issues["inspection"].append({"field": "pi_next_inspection_date", "reason": "null or empty"})

        except Exception as e:
            logger.exception("Error validating inspection: %s", e)
            return error_resp(f"Error validating inspection: {e}", 500)

        # 2b. Validate inspection_checklist
        try:
            checklist_rows = db.execute(text("SELECT * FROM inspection_checklist WHERE inspection_id = :id"), {"id": inspection_id}).fetchall() or []
            if not checklist_rows:
                issues["checklist"].append({"reason": "no checklist rows found for this inspection"})
            else:
                for r in checklist_rows:
                    rr = dict(r._mapping) if hasattr(r, "_mapping") else dict(zip(r.keys(), r))
                    row_issue = {"id": rr.get("id")}
                    for f in ("job_id", "sub_job_id", "sn", "status_id"):
                        v = rr.get(f)
                        if v is None or (isinstance(v, str) and v.strip() == ""):
                            row_issue.setdefault("missing_fields", []).append(f)
                    if "missing_fields" in row_issue:
                        issues["checklist"].append(row_issue)
        except Exception as e:
            logger.exception("Error validating checklist: %s", e)
            return error_resp(f"Error validating checklist: {e}", 500)

        # 2c. Validate to_do_list is empty
        try:
            todo_rows = db.execute(text("""
                SELECT DISTINCT c.job_id, c.job_name, t.status_id
                FROM to_do_list t
                LEFT JOIN inspection_checklist c ON t.checklist_id = c.id
                WHERE t.inspection_id = :id AND t.status_id = 2
                ORDER BY c.job_id
            """), {"id": inspection_id}).fetchall() or []
            
            if todo_rows:
                flagged_jobs = []
                for r in todo_rows:
                    rr = dict(r._mapping) if hasattr(r, "_mapping") else dict(zip(r.keys(), r))
                    job_id = rr.get("job_id")
                    job_name = rr.get("job_name")
                    if job_id is not None:
                        flagged_jobs.append({
                            "job_id": str(job_id),
                            "job_name": job_name or "",
                            "status_id": 2
                        })
                
                if flagged_jobs:
                    issues["to_do_list"] = [{
                        "reason": "to_do_list not empty - inspection has flagged items",
                        "flagged_jobs": flagged_jobs
                    }]
        except Exception as e:
            logger.exception("Error validating to_do_list: %s", e)

        # 2d. Validate images
        try:
            img_rows = db.execute(text("SELECT image_type, image_path, thumbnail_path, image_id FROM tank_images WHERE inspection_id = :id"), {"id": inspection_id}).fetchall() or []
            img_count = len(img_rows)
            
            expected_types = db.execute(text("SELECT id, image_type, count FROM image_type")).fetchall() or []
            expected_total_images = 0
            for et in expected_types:
                if hasattr(et, "_mapping"):
                    cnt = et._mapping.get("count") or 1
                elif isinstance(et, dict):
                    cnt = et.get("count") or 1
                else:
                    try:
                        _, _, cnt = et
                    except Exception:
                        cnt = 1
                expected_total_images += int(cnt)

            if expected_total_images == 0:
                expected_total_images = 15
            
            if img_count < expected_total_images:
                issues["images"].append({"reason": f"insufficient images: found {img_count}, expected {expected_total_images}"})
            else:
                for idx, r in enumerate(img_rows):
                    rr = dict(r._mapping) if hasattr(r, "_mapping") else dict(zip(r.keys(), r))
                    if not rr.get("image_path"):
                        issues["images"].append({"index": idx, "reason": "image_path missing"})
                    if (not rr.get("image_id")) and (not rr.get("image_type")):
                        issues["images"].append({"index": idx, "reason": "image type missing"})
        except Exception as e:
            logger.exception("Error validating images: %s", e)

        # 3. Check if any issues found
        any_issues = any(len(v) > 0 for v in issues.values())
        if any_issues:
            return error_resp("Cannot submit inspection - validation failed. Please complete all required fields.", 400)

        # 4. Update Status to 4 (Completed)
        db.execute(text("""
            UPDATE tank_inspection_details 
            SET status_id = 4, updated_at = NOW() 
            WHERE inspection_id = :id
        """), {"id": inspection_id})
        
        db.commit()
        
        return success_resp("Inspection submitted successfully", {"inspection_id": inspection_id, "status": "Completed"}, 200)

    except Exception as e:
        db.rollback()
        logger.error(f"Error submitting inspection {inspection_id}: {e}", exc_info=True)
        return error_resp("Failed to submit inspection", 500)

# End of file
