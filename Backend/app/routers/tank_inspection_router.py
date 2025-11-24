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

JWT_SECRET = os.getenv("JWT_SECRET", "change_this_in_production")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


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
    tank_dir = os.path.join(UPLOAD_DIR, tank_number)
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
        folder_abs = os.path.join(UPLOAD_DIR, folder)
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
        ]
        for table, val, name in master_checks:
            # try several common id column names (id, expected name, and simple variants)
            cols_to_try = ["id", name]
            # add a variant without the underscore (e.g. statusid) and without the suffix (e.g. status)
            if name and "_id" in name:
                base = name.replace("_id", "")
                cols_to_try.append(f"{base}id")
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
                    "svm": payload.safety_valve_model_id,
                    "svs": payload.safety_valve_size_id,
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
    working_pressure: Optional[float] = None
    frame_type: Optional[str] = None
    design_temperature: Optional[float] = None
    cabinet_type: Optional[str] = None
    mfgr: Optional[str] = None
    safety_valve_brand_id: Optional[int] = None
    safety_valve_model_id: Optional[int] = None      # nullable
    safety_valve_size_id: Optional[int] = None       # nullable
    notes: Optional[str] = None
    operator_id: Optional[int] = None             # nullable
    ownership: Optional[str] = None

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
        for field in [
            "inspection_date", "status_id", "inspection_type_id", "product_id", "location_id",
            "working_pressure", "frame_type", "design_temperature", "cabinet_type", "mfgr",
            "notes", "ownership"
        ]:
            if hasattr(payload, field) and getattr(payload, field) is not None:
                updates.append(f"{field} = :{field}")
                params[field] = getattr(payload, field)

        if updates:
            sql = f"UPDATE tank_inspection_details SET {', '.join(updates)}, updated_at = NOW() WHERE inspection_id = :id"
            try:
                db.execute(text(sql), params)
                db.commit()
            except Exception:
                db.rollback()
                raise

        return success_resp("Inspection details updated", {"inspection_id": inspection_id}, 200)
    except Exception as e:
        logger.error(f"Error updating tank inspection details {inspection_id}: {e}", exc_info=True)
        return error_resp("Error updating inspection details", 500)


# -------------------------
# Review endpoints (get, update, delete)
# -------------------------
@router.get("/review/{inspection_id}")
def get_inspection_review(inspection_id: int, db: Session = Depends(get_db)):
    try:
        inspection = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id"), {"id": inspection_id}).fetchone()
        if not inspection:
            return error_resp("Inspection not found", 404)

        try:
            if hasattr(inspection, "_mapping"):
                insp = dict(inspection._mapping)
            elif isinstance(inspection, dict):
                insp = inspection
            else:
                insp = dict((k, v) for k, v in inspection)
        except Exception:
            insp = jsonable_encoder(inspection)

        insp.pop("created_at", None)
        insp.pop("updated_at", None)

        # lifter thumbnail logic unchanged...
        lifter_thumb = None
        try:
            lw = insp.get("lifter_weight")
            if lw:
                folder = os.path.dirname(lw)
                tank_number = folder
                folder_abs = os.path.join(UPLOAD_DIR, folder)
                if os.path.isdir(folder_abs):
                    candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(f"{tank_number}_lifter_weight_") and fn.endswith("_thumb.jpg")]
                    if candidates:
                        candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                        lifter_thumb = f"{folder}/{candidates[0]}"
        except Exception:
            lifter_thumb = None
        insp["lifter_weight_thumbnail"] = lifter_thumb

        # tank images and checklist/to-do logic unchanged...
        tank_images_list = []
        try:
            tank_number = insp.get("tank_number")
            insp_date = insp.get("inspection_date")
            if insp_date and isinstance(insp_date, datetime):
                insp_date = insp_date.date()
            imgs = []
            try:
                if insp_date:
                    imgs = db.execute(text("SELECT * FROM tank_images WHERE tank_number = :tn AND created_date = :cd"), {"tn": tank_number, "cd": insp_date}).fetchall()
                else:
                    imgs = db.execute(text("SELECT * FROM tank_images WHERE tank_number = :tn"), {"tn": tank_number}).fetchall()
            except Exception:
                imgs = []
            folder_abs = os.path.join(UPLOAD_DIR, tank_number or "")
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
                if not img_type or str(img_type).lower() == "lifter_weight":
                    continue
                thumb_path = None
                if os.path.isdir(folder_abs):
                    prefix = f"{tank_number}_{img_type}_"
                    candidates = [fn for fn in os.listdir(folder_abs) if fn.startswith(prefix) and fn.endswith("_thumb.jpg")]
                    if candidates:
                        candidates.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_abs, fn)), reverse=True)
                        thumb_path = f"{tank_number}/{candidates[0]}"
                tank_images_list.append({"image_type": img_type, "thumbnail_path": thumb_path})
        except Exception:
            tank_images_list = []

        checklist_out = []
        todo_out = []
        try:
            # Fetch checklists and to-do items directly by inspection_id
            inspection_id_val = insp.get("inspection_id")
            if inspection_id_val:
                rows = db.execute(text("SELECT * FROM inspection_checklist WHERE inspection_id = :iid"), {"iid": inspection_id_val}).fetchall()
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
                    checklist_out.append({"job_name": rr.get("job_name"), "sub_job_name": rr.get("sub_job_description"), "status": rr.get("status"), "comment": rr.get("comment")})
                todos = db.execute(text("SELECT * FROM to_do_list WHERE inspection_id = :iid"), {"iid": inspection_id_val}).fetchall()
                for t in todos:
                    try:
                        if hasattr(t, "_mapping"):
                            tt = dict(t._mapping)
                        elif isinstance(t, dict):
                            tt = t
                        else:
                            tt = dict((k, v) for k, v in t)
                    except Exception:
                        tt = jsonable_encoder(t)
                    todo_out.append({"job_name": tt.get("job_name"), "sub_job_name": tt.get("sub_job_description"), "status": tt.get("status"), "comment": tt.get("comment")})
        except Exception:
            checklist_out = []
            todo_out = []

        resp = {
            "inspection": jsonable_encoder(insp),
            "images": [],
            "tank_images": tank_images_list,
            "inspection_checklist": checklist_out,
            "to_do_list": todo_out,
        }
        return success_resp("Inspection review fetched", resp, 200)
    except Exception as e:
        logger.error(f"Error fetching review for {inspection_id}: {e}", exc_info=True)
        return error_resp("Error fetching inspection review", 500)


class ReviewUpdateModel(BaseModel):
    inspection: Optional[dict] = None
    checklist: Optional[List[dict]] = None
    to_do: Optional[List[dict]] = None


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
                                    else:
                                        updates.append(f"{k} = :{k}")
                                        params[k] = item[k]
                            if updates:
                                db.execute(text(f"UPDATE inspection_checklist SET {', '.join(updates)} WHERE id = :id"), params)
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
                old_abs = os.path.join(UPLOAD_DIR, *old_rel.split("/"))
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
            abs_path = os.path.join(UPLOAD_DIR, *rel.split("/"))
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
