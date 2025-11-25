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
    tank_dir = os.path.join(IMAGES_ROOT_DIR, tank_number)
    os.makedirs(tank_dir, exist_ok=True)
    dst = os.path.join(tank_dir, unique_filename)
    with open(dst, "wb") as buf:
        buf.write(file.file.read())
    image_path = f"{tank_number}/{unique_filename}"

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
    status_id: int
    product_id: int
    inspection_type_id: int
    location_id: int
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None  # nullable
    safety_valve_size_id: Optional[int] = None   # nullable
    notes: Optional[str] = None
    operator_id: Optional[int] = None   # manual operator id entered by user (nullable)

    class Config:
        schema_extra = {
            "example": {
                "created_by": "user@example.com",
                "tank_id": 123,
                "status_id": 1,
                "product_id": 2,
                "inspection_type_id": 1,
                "location_id": 3,
                "safety_valve_brand_id": 2,
                "safety_valve_model_id": 1,
                "safety_valve_size_id": 1,
                "notes": "All checks ok",
                "operator_id": 1234
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
def get_active_tanks(db: Session = Depends(get_db)):
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
def get_lifter_weight_thumbnail(inspection_id: int, db: Session = Depends(get_db)):
    try:
        row = db.execute(text("SELECT lifter_weight, tank_number FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row or (hasattr(row, "_mapping") and not row._mapping.get("lifter_weight")) or (not hasattr(row, "_mapping") and not row[0]):
            return error_resp("No lifter weight photo found for this inspection.", 404)
        if hasattr(row, "_mapping"):
            tank_number = row._mapping.get("tank_number")
            rel_path = row._mapping.get("lifter_weight")
        else:
            rel_path = row[0]
            tank_number = row[1] if len(row) > 1 else None

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
    """
    Create a tank inspection record.
    - Client sends tank_id (int) in payload.
    - We resolve tank_number from tank_id and proceed as before.
    - emp_id is auto-derived from current_user.
    - operator_id may be provided by client; it must exist in operators table (operator_exists).
    - The inserted row will include tank_id and emp_id.
    """
    try:
        # --- Resolve tank_number from payload.tank_id ---
        try:
            tn_row = db.execute(
                text("SELECT tank_number FROM tank_details WHERE tank_id = :tid LIMIT 1"),
                {"tid": payload.tank_id},
            ).fetchone()
        except Exception as e:
            logger.error("DB error resolving tank_number for tank_id=%s: %s", payload.tank_id, e, exc_info=True)
            return error_resp(f"Tank not found for id: {payload.tank_id}", 404)

        if not tn_row:
            return error_resp(f"Tank not found for id: {payload.tank_id}", 404)

        if hasattr(tn_row, "_mapping"):
            tank_number = tn_row._mapping.get("tank_number")
        elif isinstance(tn_row, dict):
            tank_number = tn_row.get("tank_number")
        else:
            tank_number = tn_row[0] if len(tn_row) > 0 else None

        if not tank_number:
            return error_resp(f"Tank number missing for id: {payload.tank_id}", 404)

        # --- Validate master ids (unchanged) ---
        master_checks = [
            ("tank_status", payload.status_id, "status_id"),
            ("product_master", payload.product_id, "product_id"),
            ("inspection_type", payload.inspection_type_id, "inspection_type_id"),
            ("location_master", payload.location_id, "location_id"),
            # Optional: safety valve masters (validate if provided)
            ("safety_valve_brand", payload.safety_valve_brand_id, "safety_valve_brand_id"),
            ("safety_valve_model", payload.safety_valve_model_id, "safety_valve_model_id"),
            ("safety_valve_size", payload.safety_valve_size_id, "safety_valve_size_id"),
        ]
        # Use module-level _is_blank_or_zero helper

        for table, val, name in master_checks:
            # try several common id column names (id, expected name, and simple variants)
            cols_to_try = ["id", name]
            # add a variant without the underscore (e.g. statusid) and without the suffix (e.g. status)
            if name and "_id" in name:
                base = name.replace("_id", "")
                cols_to_try.append(f"{base}id")
                cols_to_try.append(base)

            # If the value is None or zero and the field is one of optional safety valve model/size,
            # skip validation (allow NULLs / zero means 'not selected'). Otherwise, missing values are treated as invalid.
            if name in ("safety_valve_model_id", "safety_valve_size_id") and _is_blank_or_zero(val):
                continue

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

        # --- Fetch tank details ---
        tank_details = fetch_tank_details(db, tank_number)
        inspection_date = datetime.now()
        insp_date_str = inspection_date.date().isoformat()

        # --- Resolve emp_id from current_user (auto) ---
        emp_id_val = None
        try:
            cu = current_user
            if cu:
                if hasattr(cu, "emp_id") and cu.emp_id not in (None, ""):
                    try:
                        emp_id_val = int(cu.emp_id)
                    except Exception:
                        emp_id_val = cu.emp_id
                elif hasattr(cu, "id") and cu.id not in (None, ""):
                    try:
                        emp_id_val = int(cu.id)
                    except Exception:
                        emp_id_val = cu.id
                elif isinstance(cu, dict):
                    for key in ("emp_id", "id", "user_id", "sub"):
                        if key in cu and cu.get(key) not in (None, ""):
                            try:
                                emp_id_val = int(cu.get(key))
                            except Exception:
                                emp_id_val = cu.get(key)
                            break
        except Exception:
            emp_id_val = None

        # --- operator_id is optional, no validation required (can be 0, None, or any value) ---
        # Client can send any value or omit it entirely

        # --- Prevent duplicate inspection for same tank+date+inspection_type ---
        existing = db.execute(
            text(
                "SELECT inspection_id FROM tank_inspection_details "
                "WHERE tank_number = :tn AND DATE(inspection_date) = :d AND inspection_type_id = :itype LIMIT 1"
            ),
            {"tn": tank_number, "d": inspection_date.date(), "itype": payload.inspection_type_id},
        ).fetchone()
        if existing:
            return error_resp("Inspection already exists", 400)

        # --- Generate report number and pi_next ---
        report_number = generate_report_number(db, inspection_date)
        pi_next_date = fetch_pi_next_inspection_date(db, tank_number)

        # --- Ownership logic ---
        lease_val = tank_details.get("lease")
        ownership_val = None
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

        # --- Insert the inspection record (note: include tank_id column) ---
        try:
            db.execute(
                text(
                    """
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
                    """
                ),
                {
                    "inspection_date": inspection_date,
                    "report_number": report_number,
                    "tank_number": tank_number,
                    "tank_id": payload.tank_id,
                    "status_id": payload.status_id,
                    "product_id": payload.product_id,
                    "inspection_type_id": payload.inspection_type_id,
                    "location_id": payload.location_id,
                    "working_pressure": tank_details.get("working_pressure"),
                    "frame_type": tank_details.get("frame_type"),
                    "design_temperature": tank_details.get("design_temperature"),
                    "cabinet_type": tank_details.get("cabinet_type"),
                    "mfgr": tank_details.get("mfgr"),
                    "pi_next_inspection_date": pi_next_date,
                    "svb": payload.safety_valve_brand_id,
                    "svm": (None if _is_blank_or_zero(payload.safety_valve_model_id) else payload.safety_valve_model_id),
                    "svs": (None if _is_blank_or_zero(payload.safety_valve_size_id) else payload.safety_valve_size_id),
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

        # --- Fetch inserted row and return ---
        new_row = db.execute(text("SELECT * FROM tank_inspection_details WHERE report_number = :rn"), {"rn": report_number}).fetchone()
        if not new_row:
            return error_resp("Failed to fetch created record", 500)

        try:
            if hasattr(new_row, "_mapping"):
                out = dict(new_row._mapping)
            elif isinstance(new_row, dict):
                out = new_row
            else:
                out = dict((k, v) for k, v in new_row)
        except Exception:
            try:
                out = jsonable_encoder(new_row)
            except Exception:
                out = {"report_number": report_number}

        return success_resp("Inspection created successfully", out, 201)

    except HTTPException as he:
        return error_resp(he.detail if isinstance(he.detail, str) else str(he.detail), he.status_code)
    except Exception as e:
        logger.error(f"Error creating tank inspection: {e}", exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
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
def update_tank_inspection_details(inspection_id: int, payload: TankInspectionUpdateModel, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        row = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp("Inspection not found", 404)

        params = {"id": inspection_id}
        updates = []

        # operator_id is optional, no validation required (can be 0, None, or any value)
        if hasattr(payload, "operator_id") and payload.operator_id is not None:
            updates.append("operator_id = :operator_id")
            params["operator_id"] = payload.operator_id

        # For emp_id: we set emp_id to logged-in user's emp_id if current_user present
        emp_id_val = None
        try:
            cu = current_user
            if cu:
                if hasattr(cu, "emp_id") and cu.emp_id not in (None, ""):
                    try:
                        emp_id_val = int(cu.emp_id)
                    except Exception:
                        emp_id_val = cu.emp_id
                elif hasattr(cu, "id") and cu.id not in (None, ""):
                    try:
                        emp_id_val = int(cu.id)
                    except Exception:
                        emp_id_val = cu.id
                elif isinstance(cu, dict):
                    for key in ("emp_id", "id", "user_id", "sub"):
                        if key in cu and cu.get(key) not in (None, ""):
                            try:
                                emp_id_val = int(cu.get(key))
                            except Exception:
                                emp_id_val = cu.get(key)
                            break
        except Exception:
            emp_id_val = None

        # if we have emp_id_val, update emp_id column
        if emp_id_val is not None:
            updates.append("emp_id = :emp_id")
            params["emp_id"] = emp_id_val

        # Handle optional tank_id: if provided, resolve tank_number and set both tank_id and tank_number
        if hasattr(payload, "tank_id") and payload.tank_id is not None:
            try:
                tn_row = db.execute(text("SELECT tank_number FROM tank_details WHERE tank_id = :tid LIMIT 1"), {"tid": payload.tank_id}).fetchone()
            except Exception as e:
                logger.error("DB error resolving tank_number for tank_id=%s: %s", payload.tank_id, e, exc_info=True)
                return error_resp(f"Tank not found for id: {payload.tank_id}", 404)
            if not tn_row:
                return error_resp(f"Tank not found for id: {payload.tank_id}", 404)
            if hasattr(tn_row, "_mapping"):
                tank_number_val = tn_row._mapping.get("tank_number")
            elif isinstance(tn_row, dict):
                tank_number_val = tn_row.get("tank_number")
            else:
                tank_number_val = tn_row[0] if len(tn_row) > 0 else None
            if not tank_number_val:
                return error_resp(f"Tank number missing for id: {payload.tank_id}", 404)
            updates.append("tank_id = :tank_id")
            updates.append("tank_number = :tank_number")
            params["tank_id"] = payload.tank_id
            params["tank_number"] = tank_number_val

        # update other allowed fields
        # Helper: detect if field was provided in payload (works for pydantic v1/v2)
        def payload_has_field(p, name: str) -> bool:
            try:
                if hasattr(p, "__fields_set__") and name in getattr(p, "__fields_set__"):
                    return True
                if hasattr(p, "__pydantic_fields_set__") and name in getattr(p, "__pydantic_fields_set__"):
                    return True
                # Fallback: check __dict__ presence
                if name in getattr(p, "__dict__", {}):
                    return True
            except Exception:
                return False
            return False

        for field in [
            "inspection_date", "status_id", "inspection_type_id", "product_id", "location_id",
            "working_pressure", "frame_type", "design_temperature", "cabinet_type", "mfgr",
            "notes", "ownership", "safety_valve_brand_id", "safety_valve_model_id", "safety_valve_size_id"
        ]:
            # For the safety_valve_model_id and safety_valve_size_id, allow explicit nulls (so updates can set NULL).
            if field in ("safety_valve_model_id", "safety_valve_size_id"):
                if payload_has_field(payload, field):
                    updates.append(f"{field} = :{field}")
                    v = getattr(payload, field)
                    params[field] = None if _is_blank_or_zero(v) else v
            else:
                if hasattr(payload, field) and getattr(payload, field) is not None:
                    updates.append(f"{field} = :{field}")
                    params[field] = getattr(payload, field)

        if updates:
            sql = f"UPDATE tank_inspection_details SET {', '.join(updates)}, updated_at = NOW() WHERE inspection_id = :id"
            try:
                # Validate provided safety valve master IDs (if present and not None)
                try:
                    for sf_table, sf_col in (("safety_valve_model", "safety_valve_model_id"), ("safety_valve_size", "safety_valve_size_id")):
                            # treat 0 or blank as NULL / not provided
                            if sf_col in params and params.get(sf_col) not in (None, '', 0, '0'):
                                v = params.get(sf_col)
                            res = db.execute(text(f"SELECT 1 FROM {sf_table} WHERE id = :id LIMIT 1"), {"id": v}).fetchone()
                            if not res:
                                db.rollback()
                                return error_resp(f"Invalid {sf_col}: {v}", 400)
                except Exception:
                    # validation best-effort; if unexpected error, log and proceed
                    logger.debug("Safety valve validation error", exc_info=True)
                db.execute(text(sql), params)
                db.commit()
            except Exception:
                db.rollback()
                raise

        return success_resp("Inspection details updated", {"inspection_id": inspection_id}, 200)
    except Exception as e:
        logger.error(f"Error updating tank inspection details {inspection_id}: {e}", exc_info=True)
        return error_resp("Error updating inspection details", 500)
from pydantic import BaseModel
from typing import Optional

class ReviewUpdateModel(BaseModel):
    notes: Optional[str] = None
    working_pressure: Optional[float] = None
    design_temperature: Optional[float] = None
    frame_type: Optional[str] = None
    cabinet_type: Optional[str] = None
    mfgr: Optional[str] = None
    pi_next_inspection_date: Optional[str] = None
    status: Optional[str] = None
    product: Optional[str] = None
    inspection_type: Optional[str] = None
    location: Optional[str] = None
    safety_valve_brand: Optional[str] = None
    safety_valve_model: Optional[str] = None
    safety_valve_size: Optional[str] = None


# File: /mnt/data/tank_inspection_router.py
# Replace the existing @router.get("/review/{inspection_id}") handler with this complete function.

@router.get("/review/{inspection_id}")
def get_inspection_review(inspection_id: int, db: Session = Depends(get_db)):
    try:
        inspection = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not inspection:
            return error_resp("Inspection not found", 404)

        # Normalize inspection row into a dict
        try:
            if hasattr(inspection, "_mapping"):
                insp = dict(inspection._mapping)
            elif isinstance(inspection, dict):
                insp = inspection
            else:
                insp = dict((k, v) for k, v in inspection)
        except Exception:
            insp = jsonable_encoder(inspection)

        # remove DB timestamps we don't want returned directly
        insp.pop("created_at", None)
        insp.pop("updated_at", None)

        # lifter thumbnail logic (robust)
        lifter_thumb = None
        try:
            lw = insp.get("lifter_weight")
            if lw:
                folder = os.path.dirname(lw)
                tank_number = folder
                folder_abs = os.path.join(IMAGES_ROOT_DIR, folder)
                if not os.path.isdir(folder_abs):
                    old_folder_abs = os.path.join(UPLOAD_DIR, folder)
                    if os.path.isdir(old_folder_abs):
                        folder_abs = old_folder_abs
                if os.path.isdir(folder_abs):
                    candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")]
                    if candidates:
                        candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                        lifter_thumb = f"{folder}/{candidates[0]}"
        except Exception:
            lifter_thumb = None
        insp["lifter_weight_thumbnail"] = lifter_thumb

        # Prepare images list (full entries)
        images_list = []
        try:
            tank_number = insp.get("tank_number")
            insp_date = insp.get("inspection_date")
            if insp_date and isinstance(insp_date, datetime):
                insp_date = insp_date.date()

            # define folder_abs to use for thumbnail lookup
            folder_abs = os.path.join(IMAGES_ROOT_DIR, tank_number or "")
            if not os.path.isdir(folder_abs):
                old_folder_abs = os.path.join(UPLOAD_DIR, tank_number or "")
                if os.path.isdir(old_folder_abs):
                    folder_abs = old_folder_abs

            imgs = []
            try:
                if insp_date:
                    imgs = db.execute(text("SELECT * FROM tank_images WHERE tank_number = :tn AND created_date = :cd"), {"tn": tank_number, "cd": insp_date}).fetchall()
                else:
                    imgs = db.execute(text("SELECT * FROM tank_images WHERE tank_number = :tn"), {"tn": tank_number}).fetchall()
            except Exception:
                imgs = []

            for im in imgs:
                try:
                    if hasattr(im, "_mapping"):
                        imd = dict(im._mapping)
                    elif isinstance(im, dict):
                        imd = im
                    else:
                        imd = dict((k, v) for k, v in im)
                except Exception:
                    imd = jsonable_encoder(im)

                img_type = imd.get("image_type")
                # skip lifter_weight image (kept separately)
                if not img_type or str(img_type).lower() == "lifter_weight":
                    continue

                thumb_path = imd.get("thumbnail_path")
                if not thumb_path:
                    try:
                        if os.path.isdir(folder_abs):
                            slug_type = str(img_type).strip().lower().replace(" ", "_")
                            prefix = f"{tank_number}_{slug_type}_"
                            candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(prefix) and fn.endswith("_thumb.jpg")]
                            if candidates:
                                candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                                thumb_path = f"{tank_number}/{candidates[0]}"
                    except Exception:
                        thumb_path = None

                images_list.append({
                    "image_type": img_type,
                    "image_path": imd.get("image_path"),
                    "thumbnail_path": thumb_path
                })
        except Exception:
            images_list = []

        # Fetch checklist rows for the inspection (we'll use these to build both inspection_checklist and grouped to_do_list)
        checklist_out = []
        try:
            inspection_id_val = insp.get("inspection_id")

            # build inspection_status_map for status_name lookups
            try:
                inspection_status_rows = db.execute(text("SELECT status_id, status_name FROM inspection_status")).fetchall()
                inspection_status_map = {}
                for r in inspection_status_rows:
                    try:
                        if hasattr(r, "_mapping"):
                            k = r._mapping.get("status_id")
                            v = r._mapping.get("status_name")
                        elif isinstance(r, dict):
                            k = r.get("status_id")
                            v = r.get("status_name")
                        else:
                            k = r[0] if len(r) > 0 else None
                            v = r[1] if len(r) > 1 else None
                        inspection_status_map[k] = v
                    except Exception:
                        continue
            except Exception:
                inspection_status_map = {}

            if inspection_id_val:
                rows = db.execute(text("SELECT * FROM inspection_checklist WHERE inspection_id = :iid ORDER BY id ASC"), {"iid": inspection_id_val}).fetchall()
                for r in rows:
                    try:
                        if hasattr(r, "_mapping"):
                            rr = dict(r._mapping)
                        elif isinstance(r, dict):
                            rr = r
                        else:
                            rr = dict((k, v) for k, v in r)
                    except Exception:
                        rr = jsonable_encoder(r)

                    checklist_out.append({
                        "id": rr.get("id"),
                        "job_id": rr.get("job_id"),
                        "job_name": rr.get("job_name"),
                        "sub_job_name": rr.get("sub_job_description"),
                        "sub_job_id": rr.get("sub_job_id"),
                        "sn": rr.get("sn"),
                        "status_id": rr.get("status_id") if rr.get("status_id") is not None else None,
                        "status": rr.get("status"),
                        "comment": rr.get("comment"),
                    })
        except Exception:
            checklist_out = []

        # Map top-level inspection *_id fields to friendly names (conservative; keep existing fields)
        try:
            pid = insp.pop("product_id", None)
            if pid is not None:
                try:
                    prod_rows = db.execute(text("SELECT product_id, product_name FROM product_master")).fetchall()
                    product_map = { (r._mapping.get("product_id") if hasattr(r, "_mapping") else r[0]): (r._mapping.get("product_name") if hasattr(r, "_mapping") else r[1]) for r in prod_rows }
                    insp["product"] = product_map.get(pid) if product_map.get(pid) is not None else insp.get("product_id")
                except Exception:
                    insp["product"] = insp.get("product")
        except Exception:
            pass

        # Group checklist rows into 'inspection_checklist' sections and dedupe duplicates
        grouped_sections = []
        try:
            from collections import OrderedDict
            real_job_ids = set()
            for x in checklist_out:
                jid = x.get("job_id")
                if jid is not None:
                    try:
                        if str(jid).isdigit():
                            real_job_ids.add(int(jid))
                    except Exception:
                        pass
            use_real_job_ids = len(real_job_ids) > 0

            job_groups = OrderedDict()
            seen_per_job = {}

            for it in checklist_out:
                job_id_val = it.get("job_id")
                job_name = it.get("job_name") or "Other"

                if use_real_job_ids:
                    if job_id_val is None or not str(job_id_val).isdigit() or int(job_id_val) not in real_job_ids:
                        continue
                    job_key = int(job_id_val)
                else:
                    job_key = job_id_val if job_id_val is not None else job_name

                if job_key not in job_groups:
                    job_groups[job_key] = {
                        "job_id": int(job_key) if use_real_job_ids else None,
                        "title": job_name,
                        "status_name": "",
                        "items": []
                    }
                    seen_per_job[job_key] = set()

                # derive item-level status name (if any)
                item_status_name = ""
                s_id = it.get("status_id")
                if s_id is not None:
                    item_status_name = inspection_status_map.get(s_id) or ""
                elif it.get("status"):
                    item_status_name = str(it.get("status")) or ""

                sub_job_id_val = it.get("sub_job_id")
                sn_val = it.get("sn") or ""
                dedupe_key = (None if sub_job_id_val is None else (int(sub_job_id_val) if str(sub_job_id_val).isdigit() else str(sub_job_id_val)), str(sn_val))

                if dedupe_key in seen_per_job[job_key]:
                    # update group status_name if empty and item has a status
                    if not job_groups[job_key]["status_name"] and item_status_name:
                        job_groups[job_key]["status_name"] = item_status_name
                    continue

                seen_per_job[job_key].add(dedupe_key)

                job_groups[job_key]["items"].append({
                    "sn": sn_val,
                    "title": it.get("sub_job_name") or "",
                    "comments": it.get("comment") or "",
                    "sub_job_id": sub_job_id_val if sub_job_id_val is not None else None
                })

                if not job_groups[job_key]["status_name"] and item_status_name:
                    job_groups[job_key]["status_name"] = item_status_name

            if use_real_job_ids:
                for k in sorted(job_groups.keys()):
                    grp = job_groups[k]
                    if grp.get("status_name") is None:
                        grp["status_name"] = ""
                    grouped_sections.append({
                        "job_id": grp["job_id"],
                        "title": grp["title"],
                        "status_name": grp["status_name"] or "",
                        "items": grp["items"]
                    })
            else:
                for grp in job_groups.values():
                    if grp.get("status_name") is None:
                        grp["status_name"] = ""
                    grouped_sections.append({
                        "job_id": None,
                        "title": grp["title"],
                        "status_name": grp["status_name"] or "",
                        "items": grp["items"]
                    })
        except Exception:
            grouped_sections = []

        # -----------------------
        # Build to_do_list grouped as requested, with mapped status_name for each group.
        # Group contains job_id (string when numeric), title, status_name (mapped from items when possible),
        # and items array with faulty items only.
        # -----------------------
        to_do_list_grouped = []
        try:
            from collections import OrderedDict

            def is_faulty(it):
                s_id = it.get("status_id")
                if s_id is not None and str(s_id) == "2":
                    return True
                mapped = inspection_status_map.get(s_id)
                if mapped and mapped.strip().lower() == "faulty":
                    return True
                s = it.get("status")
                if s and str(s).strip().lower() == "faulty":
                    return True
                return False

            todo_groups = OrderedDict()
            # We'll keep order by job_id numeric ascending when possible
            for it in checklist_out:
                if not is_faulty(it):
                    continue

                job_id_val = it.get("job_id")
                job_name = it.get("job_name") or "Other"

                if job_id_val is not None and str(job_id_val).isdigit():
                    job_key = int(job_id_val)
                    job_id_out = str(int(job_id_val))
                else:
                    job_key = job_name
                    job_id_out = None

                if job_key not in todo_groups:
                    todo_groups[job_key] = {
                        "job_id": job_id_out,
                        "title": job_name,
                        # default blank; we'll try to derive from items' status names (prefer a common name)
                        "status_name": "",
                        "items": [],
                        "_seen": set(),
                        "_status_names": []
                    }

                sub_job_id_val = it.get("sub_job_id")
                sn_val = it.get("sn") or ""
                dedupe_key = (None if sub_job_id_val is None else (int(sub_job_id_val) if str(sub_job_id_val).isdigit() else str(sub_job_id_val)), str(sn_val))

                if dedupe_key in todo_groups[job_key]["_seen"]:
                    # collect item-level status_name if present for group mapping
                    s_id = it.get("status_id")
                    sname = inspection_status_map.get(s_id) if s_id is not None else (it.get("status") or "")
                    if sname:
                        todo_groups[job_key]["_status_names"].append(sname)
                    continue

                todo_groups[job_key]["_seen"].add(dedupe_key)

                # determine item-level status_name (mapped)
                item_status_name = ""
                s_id = it.get("status_id")
                if s_id is not None:
                    item_status_name = inspection_status_map.get(s_id) or ""
                elif it.get("status"):
                    item_status_name = str(it.get("status")) or ""

                if item_status_name:
                    todo_groups[job_key]["_status_names"].append(item_status_name)

                todo_groups[job_key]["items"].append({
                    "sn": sn_val,
                    "title": it.get("sub_job_name") or "",
                    "comments": it.get("comment") or "",
                    "sub_job_id": sub_job_id_val if sub_job_id_val is not None else None
                })

            # finalize groups: decide group-level status_name from collected item names
            for k in todo_groups.keys():
                grp = todo_groups[k]
                status_name_out = ""
                # If multiple item-level status names exist, choose the most common; else take first; if none, keep blank
                if grp["_status_names"]:
                    # simple frequency choice
                    from collections import Counter
                    cnt = Counter([s for s in grp["_status_names"] if s])
                    if cnt:
                        status_name_out = cnt.most_common(1)[0][0] or ""
                entry = {
                    "job_id": grp["job_id"],
                    "title": grp["title"],
                    "status_name": status_name_out,
                    "items": grp["items"]
                }
                to_do_list_grouped.append(entry)
        except Exception:
            to_do_list_grouped = []

        # Build response (tank_images intentionally omitted per previous request)
        resp = {
            "inspection": jsonable_encoder(insp),
            "images": images_list,
            "inspection_checklist": grouped_sections,
            "to_do_list": to_do_list_grouped,
        }
        return success_resp("Inspection review fetched", resp, 200)

    except Exception as e:
        logger.error(f"Error fetching review for {inspection_id}: {e}", exc_info=True)
        return error_resp("Error fetching inspection review", 500)


@router.put("/review/{inspection_id}")
def update_inspection_review(inspection_id: int, payload: ReviewUpdateModel, db: Session = Depends(get_db), current_user: Optional[dict] = Depends(get_current_user)):
    try:
        row = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp("Inspection not found", 404)

        if payload.inspection:
            upd_pairs = []
            params = {"id": inspection_id}
            for k, v in payload.inspection.items():
                if k in ("created_at", "updated_at", "inspection_id"):
                    continue
                # operator_id can be any value (0, None, etc.) - no validation needed
                upd_pairs.append(f"{k} = :{k}")
                params[k] = v
            if upd_pairs:
                sql = f"UPDATE tank_inspection_details SET {', '.join(upd_pairs)}, updated_at = NOW() WHERE inspection_id = :id"
                try:
                    db.execute(text(sql), params)
                    db.commit()
                except Exception:
                    db.rollback()

        # fetch original row mapping
        try:
            if hasattr(row, "_mapping"):
                insp = dict(row._mapping)
            elif isinstance(row, dict):
                insp = row
            else:
                insp = dict((k, v) for k, v in row)
        except Exception:
            insp = jsonable_encoder(row)

        # Update checklist items directly using inspection_id
        if payload.checklist:
            try:
                inspection_id_val = insp.get("inspection_id")
                for item in payload.checklist:
                    try:
                        if item.get("id"):
                            updates = []
                            params = {"id": item["id"]}
                            for k in ("job_name", "status", "comment", "sub_job_name"):
                                if k in item:
                                    if k == "sub_job_name":
                                        updates.append("sub_job_description = :sub_job_name")
                                        params["sub_job_name"] = item[k]
                                    elif k == "status":
                                        # If the client supplied a numeric status, update status_id and flagged; else set textual status
                                        try:
                                            sid = int(item[k])
                                            updates.append("status_id = :status_id")
                                            params["status_id"] = sid
                                            updates.append("flagged = :flagged")
                                            params["flagged"] = 1 if sid in FAULTY_STATUS_IDS else 0
                                        except Exception:
                                            updates.append("status = :status")
                                            params["status"] = item[k]
                                    else:
                                        updates.append(f"{k} = :{k}")
                                        params[k] = item[k]
                            if updates:
                                db.execute(text(f"UPDATE inspection_checklist SET {', '.join(updates)} WHERE id = :id"), params)
                                # After updating the checklist row, ensure to sync flagged state to to_do_list
                                # Re-sync the checklist row using the same SQLAlchemy transaction
                                try:
                                    sel = db.execute(text(
                                        "SELECT id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at "
                                        "FROM inspection_checklist WHERE id = :id AND flagged = 1"
                                    ), {"id": params["id"]}).mappings().fetchone()
                                    if sel:
                                        # Upsert details into to_do_list within same transaction
                                        db.execute(text(
                                            "INSERT INTO to_do_list (checklist_id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at) "
                                            "VALUES (:checklist_id, :inspection_id, :tank_id, :job_name, :sub_job_description, :sn, :status_id, :comment, :created_at) "
                                            "ON DUPLICATE KEY UPDATE inspection_id=VALUES(inspection_id), tank_id=VALUES(tank_id), job_name=VALUES(job_name), sub_job_description=VALUES(sub_job_description), status_id=VALUES(status_id), comment=VALUES(comment)"
                                        ), {
                                            "checklist_id": sel["id"],
                                            "inspection_id": sel["inspection_id"],
                                            "tank_id": sel["tank_id"],
                                            "job_name": sel["job_name"],
                                            "sub_job_description": sel["sub_job_description"],
                                            "sn": sel["sn"] or "",
                                            "status_id": sel["status_id"],
                                            "comment": sel["comment"],
                                            "created_at": sel["created_at"]
                                        })
                                    else:
                                        # If not flagged, ensure there's no stale to-do entry
                                        db.execute(text("DELETE FROM to_do_list WHERE checklist_id = :chkid"), {"chkid": params["id"]})
                                except Exception:
                                    logger.exception("Failed to sync checklist (sqlalchemy) to to_do_list (non-fatal)")
                        else:
                            continue
                    except Exception:
                        db.rollback()
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception:
                pass

        # Update to-do items directly using inspection_id
        if payload.to_do:
            try:
                inspection_id_val = insp.get("inspection_id")
                for t in payload.to_do:
                    try:
                        if t.get("id"):
                            updates = []
                            params = {"id": t["id"]}
                            for k in ("job_name", "status", "comment", "sub_job_name"):
                                if k in t:
                                    if k == "sub_job_name":
                                        updates.append("sub_job_description = :sub_job_name")
                                        params["sub_job_name"] = t[k]
                                    elif k == "status":
                                        # to_do_list now stores a numeric status id in column status_id
                                        updates.append("status_id = :status")
                                        params["status"] = t[k]
                                    else:
                                        updates.append(f"{k} = :{k}")
                                        params[k] = t[k]
                            if updates:
                                db.execute(text(f"UPDATE to_do_list SET {', '.join(updates)} WHERE id = :id"), params)
                        else:
                            continue
                    except Exception:
                        db.rollback()
                try:
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception:
                pass

        return success_resp("Review updated", {"inspection_id": inspection_id}, 200)
    except Exception as e:
        logger.error(f"Error updating review for {inspection_id}: {e}", exc_info=True)
        return error_resp(str(e), 500)


@router.delete("/review/{inspection_id}")
def delete_inspection_review(inspection_id: int, db: Session = Depends(get_db)):
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
def upload_lifter_weight(inspection_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        row = db.execute(text("SELECT inspection_id, tank_number, lifter_weight FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)
        if hasattr(row, "_mapping"):
            tank_number = row._mapping.get("tank_number")
            old_rel = row._mapping.get("lifter_weight")
        else:
            try:
                old_rel = row[2]
                tank_number = row[1]
            except Exception:
                old_rel = None
                tank_number = None

        if not file.content_type or not file.content_type.startswith("image/"):
            return error_resp("File must be an image", 400)

        saved = _save_lifter_file(file, tank_number)
        rel_path = saved["image_path"]
        thumb_path = saved.get("thumbnail_path")

        try:
            if old_rel:
                old_abs = os.path.join(IMAGES_ROOT_DIR, *old_rel.split("/"))
                if os.path.exists(old_abs):
                    try:
                        os.remove(old_abs)
                    except Exception:
                        logger.debug("Could not remove old lifter file: %s", old_abs, exc_info=True)
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
            logger.debug("Error while cleaning old lifter files for inspection %s", inspection_id, exc_info=True)

        try:
            db.execute(text("UPDATE tank_inspection_details SET lifter_weight = :lp, updated_at = NOW() WHERE inspection_id = :id"), {"lp": rel_path, "id": inspection_id})
            db.commit()
        except Exception:
            db.rollback()
            logger.error("Failed to update lifter_weight column", exc_info=True)
            return error_resp("Failed to save lifter weight path to DB", 500)

        return success_resp("Lifter weight photo uploaded", {"inspection_id": inspection_id, "lifter_weight": rel_path, "thumbnail": thumb_path}, 200)
    except Exception as e:
        logger.error(f"Error uploading lifter weight for inspection {inspection_id}: {e}", exc_info=True)
        return error_resp("Error uploading lifter weight", 500)


@router.delete("/delete/inspection_details/{inspection_id}")
def delete_inspection_details(inspection_id: int, db: Session = Depends(get_db)):
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
def get_tank_details(tank_id: int, db: Session = Depends(get_db)):
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
def get_inspection_by_id(inspection_id: int, db: Session = Depends(get_db)):
    """
    Fetch inspection record by inspection_id with all required fields.
    """
    try:
        row = db.execute(
            text("""
                SELECT 
                    inspection_id,
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

        # Map safety valve ids => names and remove numeric ids (keep inspection_id only)
        try:
            # Prepare maps or fetch directly
            def fetch_one_field(table, id_col, name_col, id_val):
                if id_val is None:
                    return None
                try:
                    r = db.execute(text(f"SELECT {name_col} FROM {table} WHERE {id_col} = :id LIMIT 1"), {"id": id_val}).fetchone()
                    if not r:
                        return None
                    if hasattr(r, "_mapping"):
                        return r._mapping.get(name_col)
                    elif isinstance(r, dict):
                        return r.get(name_col)
                    else:
                        return r[0] if len(r) > 0 else None
                except Exception:
                    return None

            # status
            sid = out.pop("status_id", None)
            if sid is not None:
                st_name = fetch_one_field("tank_status", "status_id", "status_name", sid)
                out["status"] = st_name or out.get("status")
            # product
            pid = out.pop("product_id", None)
            if pid is not None:
                out["product"] = fetch_one_field("product_master", "product_id", "product_name", pid)
            # inspection type
            itid = out.pop("inspection_type_id", None)
            if itid is not None:
                out["inspection_type"] = fetch_one_field("inspection_type", "inspection_type_id", "inspection_type_name", itid)
            # location
            lid = out.pop("location_id", None)
            if lid is not None:
                out["location"] = fetch_one_field("location_master", "location_id", "location_name", lid)
            # safety valve brand/model/size
            b = out.pop("safety_valve_brand_id", None)
            if b is not None:
                out["safety_valve_brand"] = fetch_one_field("safety_valve_brand", "id", "brand_name", b)
            m = out.pop("safety_valve_model_id", None)
            if m is not None:
                out["safety_valve_model"] = fetch_one_field("safety_valve_model", "id", "model_name", m)
            s = out.pop("safety_valve_size_id", None)
            if s is not None:
                out["safety_valve_size"] = fetch_one_field("safety_valve_size", "id", "size_label", s)
            # operator -> operator_name
            op = out.pop("operator_id", None)
            if op is not None:
                out["operator"] = fetch_one_field("operators", "operator_id", "operator_name", op)
            # emp_id -> user name/email
            emp = out.pop("emp_id", None)
            if emp is not None:
                try:
                    u = db.execute(text("SELECT emp_id, name, email FROM users WHERE emp_id = :id LIMIT 1"), {"id": emp}).fetchone()
                    if u:
                        if hasattr(u, "_mapping"):
                            out["emp_name"] = u._mapping.get("name") or u._mapping.get("email")
                        elif isinstance(u, dict):
                            out["emp_name"] = u.get("name") or u.get("email")
                        else:
                            out["emp_name"] = u[1] if len(u) > 1 else u[0]
                except Exception:
                    out["emp_name"] = None
            # remove tank_id -> we still return tank_number
            out.pop("tank_id", None)
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
def delete_lifter_weight(inspection_id: int, db: Session = Depends(get_db)):
    try:
        row = db.execute(text("SELECT inspection_id, tank_number, lifter_weight FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not row:
            return error_resp(f"Inspection {inspection_id} not found", 404)

        if hasattr(row, "_mapping"):
            rel = row._mapping.get("lifter_weight")
        else:
            try:
                rel = row[2]
            except Exception:
                rel = None

        if not rel:
            return error_resp("No lifter weight image present for this inspection", 404)

        try:
            abs_path = os.path.join(IMAGES_ROOT_DIR, *rel.split("/"))
            folder = os.path.dirname(abs_path)
            base_no_ext = os.path.splitext(os.path.basename(abs_path))[0]

            if os.path.exists(abs_path):
                try:
                    os.remove(abs_path)
                except Exception:
                    logger.debug("Could not remove lifter file %s", abs_path, exc_info=True)

            if os.path.isdir(folder):
                for fn in os.listdir(folder):
                    if base_no_ext in fn and "thumb" in fn:
                        try:
                            os.remove(os.path.join(folder, fn))
                        except Exception:
                            pass
        except Exception:
            logger.debug("Error while deleting lifter files on disk for inspection %s", inspection_id, exc_info=True)

        try:
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

# End of file
