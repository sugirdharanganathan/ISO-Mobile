# app/routers/tank_checkpoints_router.py (clean)
from fastapi import APIRouter, HTTPException, Depends, Body, Header, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Union
from pymysql.cursors import DictCursor
from app.database import get_db_connection, get_db
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
import jwt
import os

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tank_checkpoints", tags=["tank_checkpoints"])

JWT_SECRET = os.getenv("JWT_SECRET", "replace-with-real-secret")
# statuses considered 'faulty' (adjust as needed)
FAULTY_STATUS_IDS = {2}


def _success(data=None, message: str = "Operation successful"):
    return JSONResponse(status_code=200, content={"success": True, "message": message, "data": data or {}})


def _error(message: str = "Error", status_code: int = 400):
    return JSONResponse(status_code=status_code, content={"success": False, "message": message, "data": {}})


# -------------------------
# Schemas
# -------------------------

class ChecklistUpdate(BaseModel):
    inspection_id: Optional[int] = None
    tank_id: Optional[int] = None
    job_id: int = Field(...)
    sub_job_id: int = Field(...)
    status_id: Optional[Union[int, str]] = None
    comment: Optional[str] = None


class ChecklistDelete(BaseModel):
    inspection_id: Optional[int] = None
    tank_id: int = Field(...)
    job_id: int = Field(...)
    sub_job_id: int = Field(...)


class ChecklistDeleteByInspection(BaseModel):
    inspection_id: int


class SubJobItem(BaseModel):
    sub_job_id: Optional[Union[int, str]] = None
    sn: Optional[str] = None
    title: Optional[str] = None
    comments: Optional[str] = None


class FullChecklistSection(BaseModel):
    sn: Optional[str] = None
    job_id: Optional[Union[int, str]] = None
    title: Optional[str] = None
    status_id: Optional[Union[int, str]] = None
    items: List[SubJobItem]


class FullInspectionChecklistCreate(BaseModel):
    tank_id: int
    sections: List[FullChecklistSection]
    
    class Config:
        schema_extra = {
            "example": {
                "tank_id": " ",
                "sections": [
                    {
                        "sn": "1",
                        "job_id": "1",
                        "title": "Tank Body & Frame Condition",
                        "status_id": "",
                        "items": [
                            {"sn": "1.1", "title": "Body x 6 Sides & All Frame – No Dent / No Bent / No Deep Cut", "job_id": "1", "sub_job_id": "1"},
                            {"sn": "1.2", "title": "Cabin Door & Frame Condition – No Damage / Can Lock", "job_id": "1", "sub_job_id": "2"},
                            {"sn": "1.3", "title": "Tank Number, Product & Hazchem Label – Not Missing or Tear", "job_id": "1", "sub_job_id": "3"},
                            {"sn": "1.4", "title": "Condition of Paint Work & Cleanliness – Clean / No Bad Rust", "job_id": "1", "sub_job_id": "4"},
                            {"sn": "1.5", "title": "Others", "job_id": "1", "sub_job_id": "5"}
                        ]
                    },
                    {
                        "sn": "2",
                        "job_id": "2",
                        "title": "Pipework & Installation",
                        "status_id": "",
                        "items": [
                            {"sn": "2.1", "title": "Pipework Supports / Brackets – Not Loose / No Bent", "job_id": "2", "sub_job_id": "1"},
                            {"sn": "2.2", "title": "Pipework Joint & Welding – No Crack / No Icing / No Leaking", "job_id": "2", "sub_job_id": "2"},
                            {"sn": "2.3", "title": "Earthing Point", "job_id": "2", "sub_job_id": "3"},
                            {"sn": "2.4", "title": "PBU Support & Flange Connection – No Leak / Not Damage", "job_id": "2", "sub_job_id": "4"},
                            {"sn": "2.5", "title": "Others", "job_id": "2", "sub_job_id": "5"}
                        ]
                    },
                    {
                        "sn": "3",
                        "job_id": "3",
                        "title": "Tank Instrument & Assembly",
                        "status_id": "",
                        "items": [
                            {"sn": "3.1", "title": "Safety Diverter Valve – Switching Lever", "job_id": "3", "sub_job_id": "1"},
                            {"sn": "3.2", "title": "Safety Valves Connection & Joint – No Leaks", "job_id": "3", "sub_job_id": "2"},
                            {"sn": "3.3", "title": "Level & Pressure Gauge Support Bracket, Connection & Joint – Not Loosen / No Leaks", "job_id": "3", "sub_job_id": "3"},
                            {"sn": "3.4", "title": "Level & Pressure Gauge – Function Check", "job_id": "3", "sub_job_id": "4"},
                            {"sn": "3.5", "title": "Level & Pressure Gauge Valve Open / Balance Valve Close", "job_id": "3", "sub_job_id": "5"},
                            {"sn": "3.6", "title": "Data & CSC Plate – Not Missing / Not Damage", "job_id": "3", "sub_job_id": "6"},
                            {"sn": "3.7", "title": "Others", "job_id": "3", "sub_job_id": "7"}
                        ]
                    },
                    {
                        "sn": "4",
                        "job_id": "4",
                        "title": "Valves Tightness & Operation",
                        "status_id": "",
                        "items": [
                            {"sn": "4.1", "title": "Valve Handwheel – Not Missing / Nut Not Loose", "job_id": "4", "sub_job_id": "1"},
                            {"sn": "4.2", "title": "Valve Open & Close Operation – No Seizing / Not Tight / Not Jam", "job_id": "4", "sub_job_id": "2"},
                            {"sn": "4.3", "title": "Valve Tightness Incl Glands – No Leak / No Icing / No Passing", "job_id": "4", "sub_job_id": "3"},
                            {"sn": "4.4", "title": "Anchor Point", "job_id": "4", "sub_job_id": "4"},
                            {"sn": "4.5", "title": "Others", "job_id": "4", "sub_job_id": "5"}
                        ]
                    },
                    {
                        "sn": "5",
                        "job_id": "5",
                        "title": "Before Departure Check",
                        "status_id": "",
                        "items": [
                            {"sn": "5.1", "title": "All Valves Closed – Defrost & Close Firmly", "job_id": "5", "sub_job_id": "1"},
                            {"sn": "5.2", "title": "Caps fitted to Outlets or Cover from Dust if applicable", "job_id": "5", "sub_job_id": "2"},
                            {"sn": "5.3", "title": "Security Seal Fitted by Refilling Plant - Check", "job_id": "5", "sub_job_id": "3"},
                            {"sn": "5.4", "title": "Pressure Gauge – lowest possible", "job_id": "5", "sub_job_id": "4"},
                            {"sn": "5.5", "title": "Level Gauge – Within marking or standard indication", "job_id": "5", "sub_job_id": "5"},
                            {"sn": "5.6", "title": "Weight Reading – ensure within acceptance weight", "job_id": "5", "sub_job_id": "6"},
                            {"sn": "5.7", "title": "Cabin Door Lock – Secure and prevent from sudden opening", "job_id": "5", "sub_job_id": "7"},
                            {"sn": "5.8", "title": "Others", "job_id": "5", "sub_job_id": "8"}
                        ]
                    },
                    {
                        "sn": "6",
                        "job_id": "6",
                        "title": "Others Observation & Comment",
                        "status_id": "",
                        "items": []
                    }
                ]
            }
        }


# -------------------------
# Utilities
# -------------------------

def _normalize_status_id(val) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        v = val.strip()
        if v == "":
            return None
        try:
            return int(v)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"status_id must be an integer or null: {val}")
    raise HTTPException(status_code=400, detail="status_id must be an integer or null")

def _resolve_emp_id_from_users(token_sub) -> Optional[int]:
    # If token subject is numeric, assume it's an emp_id
    try:
        if token_sub is None:
            logger.debug("_resolve_emp_id_from_users: token_sub is None")
            return None
        if isinstance(token_sub, int):
            logger.debug(f"_resolve_emp_id_from_users: token_sub is int: {token_sub}")
            return token_sub
        ts = str(token_sub).strip()
        if ts.isdigit():
            logger.debug(f"_resolve_emp_id_from_users: token_sub is digit: {ts}")
            return int(ts)
    except Exception as e:
        logger.exception(f"_resolve_emp_id_from_users: error parsing token_sub: {e}")

    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # Try common username/user_id fields
            try:
                cursor.execute("SELECT emp_id FROM users WHERE username=%s LIMIT 1", (token_sub,))
            except Exception:
                try:
                    cursor.execute("SELECT emp_id FROM users WHERE user_id=%s LIMIT 1", (token_sub,))
                except Exception:
                    try:
                        cursor.execute("SELECT emp_id FROM users WHERE id=%s LIMIT 1", (token_sub,))
                    except Exception:
                        cursor.execute("SELECT emp_id FROM users LIMIT 1")
            r = cursor.fetchone()
            logger.debug(f"_resolve_emp_id_from_users: DB result: {r}")
            if r and r.get("emp_id") is not None:
                return r.get("emp_id")
    except Exception:
        logger.exception("_resolve_emp_id_from_users failed")
    finally:
        conn.close()

def _get_token_subject(Authorization: Optional[str]):
    """
    Minimal, safe replacement:
    - Accepts "Bearer <token>" or raw token.
    - Attempts decode with configured JWT_SECRET and HS256.
    - Logs decode errors for easier debugging.
    - Falls back to additional claim names: sub, subject, user, emp_id, user_id, id.
    - Returns None on any invalid/expired token (preserves current behavior).
    """
    if not Authorization:
        logger.debug("_get_token_subject: No Authorization header")
        return None
    parts = Authorization.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        token = parts[1]
    else:
        token = Authorization

    if not token:
        logger.debug("_get_token_subject: token is empty after parsing Authorization header")
        return None

    try:
        # primary decode with configured secret and HS256 (preserves current security posture)
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        logger.debug(f"_get_token_subject: Decoded payload keys: {list(payload.keys())}")
        # try common claim names (extendable)
        for claim in ("sub", "subject", "user", "emp_id", "user_id", "id"):
            if claim in payload and payload.get(claim) is not None:
                logger.debug(f"_get_token_subject: using claim '{claim}' -> {payload.get(claim)}")
                return payload.get(claim)
        # If no matching claim found, return entire payload as fallback (caller expects a simple id/string)
        # but to preserve behavior, return None (so caller still gets 401); we log payload for debugging.
        logger.debug(f"_get_token_subject: no recognized subject claim found in payload: {payload}")
        return None
    except jwt.ExpiredSignatureError:
        logger.error("_get_token_subject: Token expired")
        return None
    except jwt.InvalidAlgorithmError as e:
        logger.error(f"_get_token_subject: Invalid algorithm or algorithm mismatch: {e}")
        return None
    except jwt.InvalidTokenError as e:
        logger.error(f"_get_token_subject: Invalid token: {e}")
        return None
    except Exception as e:
        logger.exception(f"_get_token_subject: unexpected error decoding token: {e}")
        return None


def _sync_flagged_to_todo(cursor, checklist_id: int):
    """
    Sync a flagged checklist row to to_do_list.
    Ensures commit and logs errors. Returns True on success, False otherwise.
    """
    try:
        cursor.execute(
            "SELECT id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at "
            "FROM inspection_checklist WHERE id=%s LIMIT 1",
            (checklist_id,)
        )
        r = cursor.fetchone()
        if not r:
            logger.debug("_sync_flagged_to_todo: no inspection_checklist row for id=%s", checklist_id)
            return False

        # Use ON DUPLICATE KEY UPDATE to keep to_do_list in sync if you have a unique key (e.g. checklist_id)
        try:
            cursor.execute(
                """
                INSERT INTO to_do_list
                  (checklist_id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  inspection_id=VALUES(inspection_id),
                  tank_id=VALUES(tank_id),
                  job_name=VALUES(job_name),
                  sub_job_description=VALUES(sub_job_description),
                  sn=VALUES(sn),
                  status_id=VALUES(status_id),
                  comment=VALUES(comment),
                  created_at=VALUES(created_at)
                """,
                (
                    r.get("id"),
                    r.get("inspection_id"),
                    r.get("tank_id"),
                    r.get("job_name"),
                    r.get("sub_job_description"),
                    r.get("sn") or "",
                    r.get("status_id"),
                    r.get("comment"),
                    r.get("created_at"),
                ),
            )
        except Exception:
            # Fallback: some schemas may not have created_at or allow it; try inserting without created_at
            try:
                cursor.execute(
                    """
                    INSERT INTO to_do_list
                      (checklist_id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      inspection_id=VALUES(inspection_id),
                      tank_id=VALUES(tank_id),
                      job_name=VALUES(job_name),
                      sub_job_description=VALUES(sub_job_description),
                      sn=VALUES(sn),
                      status_id=VALUES(status_id),
                      comment=VALUES(comment)
                    """,
                    (
                        r.get("id"),
                        r.get("inspection_id"),
                        r.get("tank_id"),
                        r.get("job_name"),
                        r.get("sub_job_description"),
                        r.get("sn") or "",
                        r.get("status_id"),
                        r.get("comment"),
                    ),
                )
            except Exception:
                logger.exception("Failed to insert to to_do_list in _sync_flagged_to_todo (fallback)")
                return False

        # commit the change so it persists
        try:
            cursor.connection.commit()
        except Exception:
            # if commit fails log it but don't crash
            logger.exception("Failed to commit in _sync_flagged_to_todo after INSERT")
            # still return True because INSERT executed; caller can decide further action
        logger.debug("_sync_flagged_to_todo: synced checklist_id=%s to to_do_list", checklist_id)
        return True
    except Exception:
        logger.exception("_sync_flagged_to_todo failed for checklist_id=%s", checklist_id)
        return False



@router.get("/inspection_status")
def get_inspection_status():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            try:
                cursor.execute("SELECT * FROM inspection_status ORDER BY status_id")
            except Exception:
                try:
                    cursor.execute("SELECT * FROM inspection_status ORDER BY id")
                except Exception:
                    cursor.execute("SELECT * FROM inspection_status")
            raw = cursor.fetchall() or []
            rows = []
            for r in raw:
                rows.append({
                    "id": r.get("status_id") or r.get("id"),
                    "name": r.get("status_name") or r.get("name") or None,
                })
            return _success(rows)
    finally:
        conn.close()


@router.get("/export/checklist")
def export_checklist_format():
    """Return jobs and sub-jobs in the exact JSON structure of the attached `checklist.json`.

    Response shape: { "sections": [ { "sn": "1", "title": "...", "items": [ {"sn":"1.1","title":"..."}, ... ] }, ... ] }
    """
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # Fetch jobs with fallbacks for different schemas
            try:
                cursor.execute("SELECT * FROM inspection_job ORDER BY sort_order, id")
            except Exception:
                try:
                    cursor.execute("SELECT * FROM inspection_job ORDER BY id")
                except Exception:
                    cursor.execute("SELECT * FROM inspection_job")
            jobs = cursor.fetchall() or []

            sections = []
            for job in jobs:
                # determine section serial number (sn)
                sec_sn = None
                for k in ("sn", "job_id", "id", "job_code"):
                    if job.get(k) is not None:
                        sec_sn = str(job.get(k))
                        break
                if sec_sn is None:
                    sec_sn = ""

                # determine section title with fallbacks
                sec_title = job.get("job_name") or job.get("description") or job.get("job") or None

                # fetch sub-jobs for this job id (use whatever id field is present)
                jid_val = job.get("id") or job.get("job_id")
                subs = []
                if jid_val is not None:
                    try:
                        cursor.execute("SELECT * FROM inspection_sub_job WHERE job_id=%s ORDER BY COALESCE(sub_job_id, id), id", (jid_val,))
                    except Exception:
                        try:
                            cursor.execute("SELECT * FROM inspection_sub_job WHERE job_id=%s ORDER BY id", (jid_val,))
                        except Exception:
                            cursor.execute("SELECT * FROM inspection_sub_job WHERE job_id=%s", (jid_val,))
                    subs = cursor.fetchall() or []

                items = []
                for s in subs:
                    sub_sn = None
                    for k in ("sn", "sub_job_id", "id"):
                        if s.get(k) is not None:
                            sub_sn = str(s.get(k))
                            break
                    if sub_sn is None:
                        sub_sn = ""

                    sub_title = s.get("sub_job_name") or s.get("description") or s.get("sub_job") or s.get("title") or None

                    items.append({
                        "sn": sub_sn,
                        "title": sub_title,
                    })

                sections.append({
                    "sn": sec_sn,
                    "title": sec_title,
                    "items": items,
                })

            # Return exact JSON format (no extra wrapper) to match the attached `checklist.json` structure
            return JSONResponse(status_code=200, content={"sections": sections})
    finally:
        conn.close()


@router.post("/create/inspection_checklist_bulk")
def create_inspection_checklist_bulk(
    payload: FullInspectionChecklistCreate = Body(
        ...,
        example={
            "tank_id": " ",
            "sections": [
                {
                    "job_id": "1",
                    "title": "Tank Body & Frame Condition",
                    "status_id": "",
                    "items": [
                        {"sn": "1.1", "title": "Body x 6 Sides & All Frame – No Dent / No Bent / No Deep Cut", "sub_job_id": "1", "comments": ""},
                        {"sn": "1.2", "title": "Cabin Door & Frame Condition – No Damage / Can Lock", "sub_job_id": "2", "comments": ""},
                        {"sn": "1.3", "title": "Tank Number, Product & Hazchem Label – Not Missing or Tear", "sub_job_id": "3", "comments": ""},
                        {"sn": "1.4", "title": "Condition of Paint Work & Cleanliness – Clean / No Bad Rust", "sub_job_id": "4", "comments": ""},
                        {"sn": "1.5", "title": "Others", "sub_job_id": "5", "comments": ""}
                    ]
                },
                {
                    "job_id": "2",
                    "title": "Pipework & Installation",
                    "status_id": "",
                    "items": [
                        {"sn": "2.1", "title": "Pipework Supports / Brackets – Not Loose / No Bent", "comments": "", "sub_job_id": "1"},
                        {"sn": "2.2", "title": "Pipework Joint & Welding – No Crack / No Icing / No Leaking", "comments": "", "sub_job_id": "2"},
                        {"sn": "2.3", "title": "Earthing Point", "comments": "", "sub_job_id": "3"},
                        {"sn": "2.4", "title": "PBU Support & Flange Connection – No Leak / Not Damage", "comments": "", "sub_job_id": "4"},
                        {"sn": "2.5", "title": "Others", "comments": "", "sub_job_id": "5"}
                    ]
                },
                {
                    "job_id": "3",
                    "title": "Tank Instrument & Assembly",
                    "status_id": "",
                    "items": [
                        {"sn": "3.1", "title": "Safety Diverter Valve – Switching Lever", "comments": "", "sub_job_id": "1"},
                        {"sn": "3.2", "title": "Safety Valves Connection & Joint – No Leaks", "comments": "", "sub_job_id": "2"},
                        {"sn": "3.3", "title": "Level & Pressure Gauge Support Bracket, Connection & Joint – Not Loosen / No Leaks", "comments": "", "sub_job_id": "3"},
                        {"sn": "3.4", "title": "Level & Pressure Gauge – Function Check", "comments": "", "sub_job_id": "4"},
                        {"sn": "3.5", "title": "Level & Pressure Gauge Valve Open / Balance Valve Close", "comments": "", "sub_job_id": "5"},
                        {"sn": "3.6", "title": "Data & CSC Plate – Not Missing / Not Damage", "comments": "", "sub_job_id": "6"},
                        {"sn": "3.7", "title": "Others", "comments": "", "sub_job_id": "7"}
                    ]
                },
                {
                    "job_id": "4",
                    "title": "Valves Tightness & Operation",
                    "status_id": "",
                    "items": [
                        {"sn": "4.1", "title": "Valve Handwheel – Not Missing / Nut Not Loose", "comments": "", "sub_job_id": "1"},
                        {"sn": "4.2", "title": "Valve Open & Close Operation – No Seizing / Not Tight / Not Jam", "comments": "", "sub_job_id": "2"},
                        {"sn": "4.3", "title": "Valve Tightness Incl Glands – No Leak / No Icing / No Passing", "comments": "", "sub_job_id": "3"},
                        {"sn": "4.4", "title": "Anchor Point", "comments": "", "sub_job_id": "4"},
                        {"sn": "4.5", "title": "Others", "comments": "", "sub_job_id": "5"}
                    ]
                },
                {
                    "job_id": "5",
                    "title": "Before Departure Check",
                    "status_id": "",
                    "items": [
                        {"sn": "5.1", "title": "All Valves Closed – Defrost & Close Firmly", "comments": "", "sub_job_id": "1"},
                        {"sn": "5.2", "title": "Caps fitted to Outlets or Cover from Dust if applicable", "comments": "", "sub_job_id": "2"},
                        {"sn": "5.3", "title": "Security Seal Fitted by Refilling Plant - Check", "comments": "", "sub_job_id": "3"},
                        {"sn": "5.4", "title": "Pressure Gauge – lowest possible", "comments": "", "sub_job_id": "4"},
                        {"sn": "5.5", "title": "Level Gauge – Within marking or standard indication", "comments": "", "sub_job_id": "5"},
                        {"sn": "5.6", "title": "Weight Reading – ensure within acceptance weight", "comments": "", "sub_job_id": "6"},
                        {"sn": "5.7", "title": "Cabin Door Lock – Secure and prevent from sudden opening", "comments": "", "sub_job_id": "7"},
                        {"sn": "5.8", "title": "Others", "comments": "", "sub_job_id": "8"}
                    ]
                },
                {
                    "job_id": "6",
                    "title": "Others Observation & Comment",
                    "status_id": None,
                    "comment": "",
                    "items": []
                }
            ]
        }
    ),
    Authorization: Optional[str] = Header(None),
    Inspection_Id: Optional[Union[int, str]] = Header(None, alias="Inspection-Id"),
    db: Session = Depends(get_db),
):
    logger.debug(f"create_inspection_checklist_bulk: Authorization header: {Authorization}")
    token_sub = _get_token_subject(Authorization)
    logger.debug(f"create_inspection_checklist_bulk: token_sub: {token_sub}")
    if token_sub is None:
        logger.error(f"create_inspection_checklist_bulk: Authorization required (token_sub is None). Header value: {Authorization}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Authorization required. Header value: {Authorization}")
    emp_id = _resolve_emp_id_from_users(token_sub)
    logger.debug(f"create_inspection_checklist_bulk: emp_id: {emp_id}")

    # inspection_id is now provided in header `Inspection-Id`
    if Inspection_Id is None or str(Inspection_Id).strip() == "":
        logger.error("create_inspection_checklist_bulk: Inspection-Id header required")
        raise HTTPException(status_code=400, detail="Inspection-Id header required")
    try:
        inspection_id = int(str(Inspection_Id))
    except Exception:
        logger.error(f"create_inspection_checklist_bulk: Invalid Inspection-Id header value: {Inspection_Id}")
        raise HTTPException(status_code=400, detail="Invalid Inspection-Id header value")

    tank_id = payload.tank_id

    try:
        with db.begin():
            # minimal implementation: validate jobs/subjobs and insert rows
            for section in payload.sections:
                # select all columns and pick available fields (avoid unknown-column errors)
                # try by `id` first (most schemas), then fall back to `job_id` if present
                # Try numeric id lookup first (canonical). If not found, attempt to match by job_code/job_name/job_description
                job_row = None
                try:
                    # attempt numeric id match
                    job_row = db.execute(text("SELECT * FROM inspection_job WHERE id = :jid LIMIT 1"), {"jid": section.job_id}).mappings().fetchone()
                except Exception:
                    job_row = None
                if not job_row:
                    jstr = str(section.job_id).strip() if section.job_id is not None else ""
                    if jstr != "":
                        # Avoid referencing potentially-missing columns in SQL (job_code etc.).
                        # Fetch rows and match in Python across likely descriptive fields.
                        try:
                            candidate_rows = db.execute(text("SELECT * FROM inspection_job"), {}).mappings().fetchall()
                        except Exception:
                            candidate_rows = []
                        job_row = None
                        for jr in candidate_rows:
                            for key in ("job_name", "job_description", "description", "job_code"):
                                if key in jr and jr.get(key) is not None and str(jr.get(key)).strip() == jstr:
                                    job_row = jr
                                    break
                            if job_row:
                                break
                if not job_row:
                    return _error(f"Job not found: {section.job_id}", status_code=400)

                # normalize job-level status_id (client now provides status per job)
                status_id_norm = _normalize_status_id(getattr(section, 'status_id', None))
                status_id_final_job = status_id_norm if status_id_norm is not None else 1

                for item in section.items:
                    # ---------- find sub_row ----------
                    sub_row = None
                    jid_val = job_row.get("id") or job_row.get("job_id")
                    try:
                        sub_row = db.execute(
                            text("SELECT * FROM inspection_sub_job WHERE sub_job_id=:sid AND job_id=:jid LIMIT 1"),
                            {"sid": item.sub_job_id, "jid": jid_val}
                        ).mappings().fetchone()
                    except Exception:
                        sub_row = None
                    if not sub_row:
                        try:
                            sub_row = db.execute(
                                text("SELECT * FROM inspection_sub_job WHERE id=:sid AND job_id=:jid LIMIT 1"),
                                {"sid": item.sub_job_id, "jid": jid_val}
                            ).mappings().fetchone()
                        except Exception:
                            sub_row = None

                    if not sub_row:
                        # fallback matching logic (positional or name match)
                        sstr = str(item.sub_job_id).strip() if item.sub_job_id is not None else ""
                        title_str = (item.title or "").strip()
                        try:
                            candidate_subs = db.execute(
                                text("SELECT * FROM inspection_sub_job WHERE job_id = :jid ORDER BY COALESCE(sub_job_id, id), id"),
                                {"jid": jid_val}
                            ).mappings().fetchall()
                        except Exception:
                            candidate_subs = []

                        sub_row = None
                        if sstr.isdigit():
                            try:
                                sid_int = int(sstr)
                                for sr in candidate_subs:
                                    if sr.get("sub_job_id") is not None and str(sr.get("sub_job_id")) == str(sid_int):
                                        sub_row = sr
                                        break
                                if sub_row is None:
                                    idx = sid_int
                                    if idx >= 1 and idx <= len(candidate_subs):
                                        sub_row = candidate_subs[idx - 1]
                            except Exception:
                                sub_row = None

                        if sub_row is None:
                            for sr in candidate_subs:
                                if sstr != "":
                                    if ("id" in sr and str(sr.get("id")) == sstr) or ("sub_job_id" in sr and str(sr.get("sub_job_id")) == sstr):
                                        sub_row = sr
                                        break
                                if title_str:
                                    for sk in ("sub_job_name", "description", "sn"):
                                        if sk in sr and sr.get(sk) and str(sr.get(sk)).strip() == title_str:
                                            sub_row = sr
                                            break
                                    if sub_row:
                                        break

                    if not sub_row:
                        return _error(f"Sub-job not found: {item.sub_job_id}", status_code=400)

                    # use job-level status_id for all sub-jobs
                    status_id_final = status_id_final_job
                    # flagged only if status_id == 2
                    flagged_val = 1 if (status_id_final in FAULTY_STATUS_IDS) else 0
                    # Prefer the client's provided `sn` (e.g. "1.1", "4.3"); if absent, fall back to stored sn or a compact name
                    if item.sn and str(item.sn).strip() != "":
                        sn_val = str(item.sn).strip()
                    else:
                        if sub_row.get("sn"):
                            sn_val = str(sub_row.get("sn"))
                        elif sub_row.get("sub_job_name"):
                            sn_val = str(sub_row.get("sub_job_name"))[:16]
                        elif sub_row.get("id") is not None:
                            sn_val = str(sub_row.get("id"))
                        else:
                            sn_val = ""

                    # Use DB canonical job id if available, otherwise fall back to provided section.job_id (less preferable)
                    db_job_id = None
                    if job_row is not None:
                        db_job_id = job_row.get("id") or job_row.get("job_id")

                    # ---- INSERT the checklist row (moved OUT of the 'else' fallback) ----
                    db.execute(text(
                        "INSERT INTO inspection_checklist (inspection_id, tank_id, emp_id, job_id, job_name, sn, sub_job_id, sub_job_description, status_id, comment, flagged, created_at)"
                        " VALUES (:inspection_id, :tank_id, :emp_id, :job_id, :job_name, :sn, :sub_job_id, :sub_job_description, :status_id, :comment, :flagged, NOW())"
                    ), {
                        "inspection_id": inspection_id,
                        "tank_id": tank_id,
                        "emp_id": emp_id,
                        "job_id": db_job_id if db_job_id is not None else section.job_id,
                        "job_name": job_row.get("job_name") or job_row.get("job") or None,
                        "sn": sn_val,
                        "sub_job_id": sub_row.get("sub_job_id") or sub_row.get("id"),
                        "sub_job_description": getattr(item, 'title', None),
                        "status_id": status_id_final,
                        "comment": getattr(item, 'comments', None),
                        "flagged": flagged_val,
                    })

                    # Only sync flagged items (status_id == 2) into to_do_list
                    # After inserting inspection_checklist row above:
                    if flagged_val:
                        try:
                            # select the most recent row matching the unique tuple we inserted (inspection, tank, job, sub_job)
                            sel = db.execute(text(
                                "SELECT id, job_name, sub_job_description, sn, status_id, comment, created_at, tank_id, inspection_id "
                                "FROM inspection_checklist "
                                "WHERE inspection_id = :inspection_id AND tank_id = :tank_id AND job_id = :job_id AND sub_job_id = :sub_job_id "
                                "ORDER BY id DESC LIMIT 1"
                            ), {
                                "inspection_id": inspection_id,
                                "tank_id": tank_id,
                                "job_id": db_job_id if db_job_id is not None else section.job_id,
                                "sub_job_id": sub_row.get("sub_job_id") or sub_row.get("id")
                            }).mappings().fetchone()

                            if sel:
                                db.execute(text(
                                    "INSERT INTO to_do_list (checklist_id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at) "
                                    "VALUES (:checklist_id, :inspection_id, :tank_id, :job_name, :sub_job_description, :sn, :status_id, :comment, :created_at) "
                                    "ON DUPLICATE KEY UPDATE inspection_id=VALUES(inspection_id), tank_id=VALUES(tank_id), job_name=VALUES(job_name), sub_job_description=VALUES(sub_job_description), sn=VALUES(sn), status_id=VALUES(status_id), comment=VALUES(comment), created_at=VALUES(created_at)"
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
                        except Exception:
                            logger.exception("Failed to sync flagged item into to_do_list")

        db.commit()

        # Build response in exact requested format (match check_list_response.json)
        resp_sections = []
        for section in payload.sections:
            items_out = []
            for it in section.items:
                items_out.append({
                    "sn": getattr(it, 'sn', "") or "",
                    "title": getattr(it, 'title', None),
                    "job_id": str(section.job_id) if section.job_id is not None else "",
                    "sub_job_id": str(getattr(it, 'sub_job_id', "") or ""),
                })

            # section-level status_id (string or empty)
            sec_status = getattr(section, 'status_id', None)
            try:
                sec_status_str = str(sec_status) if sec_status is not None and str(sec_status).strip() != "" else ""
            except Exception:
                sec_status_str = ""

            resp_sections.append({
                "job_id": str(section.job_id) if section.job_id is not None else "",
                "title": getattr(section, 'title', None),
                "status_id": sec_status_str,
                "items": items_out,
            })

        data = {
            "inspection_id": str(inspection_id) if inspection_id is not None else "",
            "tank_id": str(tank_id) if tank_id is not None else "",
            "emp_id": str(emp_id) if emp_id is not None else "",
            "sections": resp_sections,
        }

        return _success(data, message="Checklist fetched successfully")
    except HTTPException as he:
        # preserve raised HTTPExceptions as standardized errors
        return _error(str(he.detail if hasattr(he, 'detail') else he), status_code=getattr(he, 'status_code', 400))
    except Exception as e:
        logger.exception("Error creating inspection checklist bulk")
        return _error(str(e), status_code=500)


@router.delete("/delete/inspection_checklist")
def delete_inspection_checklist(payload: ChecklistDeleteByInspection, Authorization: Optional[str] = Header(None)):
    """Delete all data associated with an inspection.

    Request body: { "inspection_id": <int> }
    Deletes rows from `inspection_checklist` and `to_do_list` matching the inspection_id.
    """
    token_subject = _get_token_subject(Authorization)
    if token_subject is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization required")
    # emp_id of caller is available if needed for audits; we resolve it to ensure the user exists
    _ = _resolve_emp_id_from_users(token_subject)

    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # verify there are entries under this inspection
            cursor.execute("SELECT COUNT(1) AS cnt FROM inspection_checklist WHERE inspection_id=%s", (payload.inspection_id,))
            r = cursor.fetchone()
            if not r or (r.get('cnt') or 0) == 0:
                raise HTTPException(status_code=404, detail="Inspection not found")

            # delete checklist entries
            cursor.execute("DELETE FROM inspection_checklist WHERE inspection_id=%s", (payload.inspection_id,))
            # delete related todo items
            try:
                cursor.execute("DELETE FROM to_do_list WHERE inspection_id=%s", (payload.inspection_id,))
            except Exception:
                # If to_do_list doesn't exist or delete fails, log and continue
                logger.exception("Failed to delete to_do_list entries for inspection_id=%s", payload.inspection_id)

            conn.commit()
            return _success({"deleted_inspection_id": payload.inspection_id}, message="Inspection deleted")
    except Exception as e:
        logger.exception("Error deleting inspection checklist")
        raise HTTPException(status_code=500, detail=f"Error deleting inspection checklist: {e}")
@router.put("/update/job_status")
def update_job_status(
    inspection_id: int = Body(..., embed=True),
    job_id: int = Body(..., embed=True),
    status_id: int = Body(..., embed=True),
    Authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Update status_id for all sub-jobs under a job_id for a given inspection_id.
    If status_id == 2, flag and sync to to_do_list.
    """
    token_sub = _get_token_subject(Authorization)
    if token_sub is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization required.")
    emp_id = _resolve_emp_id_from_users(token_sub)
    try:
        with db.begin():
            # Update all checklist items for this inspection_id and job_id
            checklist_items = db.execute(text(
                "SELECT * FROM inspection_checklist WHERE inspection_id = :inspection_id AND job_id = :job_id"
            ), {"inspection_id": inspection_id, "job_id": job_id}).mappings().fetchall()
            for item in checklist_items:
                db.execute(text(
                    "UPDATE inspection_checklist SET status_id = :status_id, flagged = :flagged WHERE id = :id"
                ), {
                    "status_id": status_id,
                    "flagged": 1 if status_id == 2 else 0,
                    "id": item["id"]
                })
                # If flagged, sync to to_do_list
                if status_id == 2:
                    sel = db.execute(text(
                        "SELECT id, job_name, sub_job_description, sn, status_id, comment, created_at, tank_id, inspection_id FROM inspection_checklist WHERE id = :id"
                    ), {"id": item["id"]}).mappings().fetchone()
                    if sel:
                        db.execute(text(
                            "INSERT INTO to_do_list (checklist_id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at) "
                            "VALUES (:checklist_id, :inspection_id, :tank_id, :job_name, :sub_job_description, :sn, :status_id, :comment, :created_at) "
                            "ON DUPLICATE KEY UPDATE inspection_id=VALUES(inspection_id), tank_id=VALUES(tank_id), job_name=VALUES(job_name), sub_job_description=VALUES(sub_job_description), sn=VALUES(sn), status_id=VALUES(status_id), comment=VALUES(comment), created_at=VALUES(created_at)"
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

        db.commit()
        return _success(message="Job status updated successfully.")
    except Exception as e:
        logger.exception("Error updating job status")
        return _error(str(e), status_code=500)


@router.get("/get/checklist_by_inspection_id/{inspection_id}")
def get_checklist_by_inspection_id(
    inspection_id: int,
    Authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Retrieve checklist data for a given inspection_id in the required JSON format.
    """
    token_sub = _get_token_subject(Authorization)
    if token_sub is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization required.")
    emp_id = _resolve_emp_id_from_users(token_sub)
    try:
        # Fetch all checklist items for the inspection_id
        checklist_items = db.execute(text(
            "SELECT * FROM inspection_checklist WHERE inspection_id = :inspection_id ORDER BY job_id, sub_job_id"
        ), {"inspection_id": inspection_id}).mappings().fetchall()
        if not checklist_items:
            return _error(f"No checklist found for inspection_id: {inspection_id}", status_code=404)

        # Build inspection status map for name resolution
        try:
            srows = db.execute(text("SELECT status_id, status_name FROM inspection_status")).mappings().fetchall()
            status_map = {r['status_id']: r['status_name'] for r in (srows or [])}
        except Exception:
            status_map = {}

        # Group by job_id
        sections = {}
        for item in checklist_items:
            job_id = str(item["job_id"])
            if job_id not in sections:
                sections[job_id] = {
                    "job_id": job_id,
                    "title": item.get("job_name"),
                    "status_name": status_map.get(item.get("status_id")) or str(item.get("status_id", "")),
                    "items": []
                }
            sections[job_id]["items"].append({
                "sn": item.get("sn", ""),
                "title": item.get("sub_job_description"),
                "job_id": job_id,
                "sub_job_id": str(item.get("sub_job_id", "")),
            })

        resp_sections = list(sections.values())
        data = {
            "inspection_id": str(inspection_id),
            "tank_id": str(checklist_items[0]["tank_id"] if checklist_items else ""),
            "emp_id": str(checklist_items[0]["emp_id"] if checklist_items else ""),
            "sections": resp_sections,
        }
        return _success(data, message="Checklist data fetched successfully.")
    except Exception as e:
        logger.exception("Error fetching checklist by inspection_id")
        return _error(str(e), status_code=500)

 