from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Any
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from app.database import get_db_connection, get_db

# Local response helpers to avoid circular imports
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
from app.models.to_do_list_model import ToDoList
from pymysql.cursors import DictCursor
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/to_do_list", tags=["to_do_list"])


# ----------------------------
# RESPONSE MODELS
class ToDoListResponse(BaseModel):
    id: int
    checklist_id: int
    inspection_id: int
    tank_id: Optional[int]
    job_name: Optional[str]
    sub_job_description: Optional[str]
    sn: str
    status_name: Optional[str]
    comment: Optional[str]
    created_at: str


class GenericResponse(BaseModel):
    success: bool
    data: List[dict]


# ----------------------------
# HELPER: SYNC FLAGGED ITEMS
# ----------------------------
def _sync_flagged_to_todo(cursor, checklist_id: int):
    """
    Sync a flagged checklist row to to_do_list.
    Now uses inspection_id instead of report_id.
    """
    cursor.execute("""
        SELECT id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at
        FROM inspection_checklist
        WHERE id=%s AND flagged=1
    """, (checklist_id,))
    row = cursor.fetchone()
    if not row:
        return
    cursor.execute("""
        INSERT INTO to_do_list (checklist_id, inspection_id, tank_id, job_name, sub_job_description, sn, status_id, comment, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            inspection_id=VALUES(inspection_id),
            tank_id=VALUES(tank_id),
            job_name=VALUES(job_name),
            sub_job_description=VALUES(sub_job_description),
            status_id=VALUES(status_id),
            comment=VALUES(comment)
    """, (
        checklist_id,
        row['inspection_id'],
            row['tank_id'],
        row['job_name'],
        row['sub_job_description'],
        row['sn'],
        row['status_id'],
        row['comment'],
        row['created_at']
    ))


# ----------------------------
# GET ALL TO-DO ITEMS
# ----------------------------
@router.get("/list", response_model=GenericResponse)
def get_to_do_list():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("""
                SELECT id, checklist_id, inspection_id, tank_id, job_name, sub_job_description,
                       sn, status_id, comment, created_at
                FROM to_do_list
                ORDER BY created_at DESC
            """)
            rows = cursor.fetchall() or []

            # Convert numeric status_id -> status_name
            # Build inspection_status map (best effort, fallback to empty string)
            status_map = {}
            try:
                cursor.execute("SELECT status_id, status_name FROM inspection_status")
                for r in cursor.fetchall() or []:
                    status_map[r.get('status_id')] = r.get('status_name')
            except Exception:
                status_map = {}

            # Build checklist_id -> (job_id, sub_job_id) map to get numeric job ids where possible
            checklist_ids = [r.get('checklist_id') for r in rows if r.get('checklist_id')]
            checklist_map = {}
            if checklist_ids:
                try:
                    fmt = ','.join(['%s'] * len(checklist_ids))
                    cursor.execute(f"SELECT id, job_id, sub_job_id FROM inspection_checklist WHERE id IN ({fmt})", tuple(checklist_ids))
                    for cr in cursor.fetchall() or []:
                        checklist_map[cr.get('id')] = cr
                except Exception:
                    checklist_map = {}

            # Group rows into job groups
            from collections import OrderedDict, Counter
            groups = OrderedDict()
            for r in rows:
                chk_id = r.get('checklist_id')
                job_id = None
                sub_id = None
                if chk_id and chk_id in checklist_map:
                    job_id = checklist_map[chk_id].get('job_id')
                    sub_id = checklist_map[chk_id].get('sub_job_id')

                # prefer numeric job_id where available, else use job_name as key
                if job_id is not None and str(job_id).isdigit():
                    job_key = int(job_id)
                    job_id_out = str(int(job_id))
                else:
                    job_key = r.get('job_name') or "Other"
                    job_id_out = None

                title = r.get('job_name') or "Other"

                if job_key not in groups:
                    groups[job_key] = {"job_id": job_id_out, "title": title, "status_names": [], "items": [], "_seen": set()}

                # dedupe by (sub_job_id, sn)
                sn_val = r.get('sn') or ""
                dedupe_key = (None if sub_id is None else (int(sub_id) if str(sub_id).isdigit() else str(sub_id)), str(sn_val))
                if dedupe_key in groups[job_key]["_seen"]:
                    # collect status_name for later aggregation
                    sname = status_map.get(r.get('status_id')) or ""
                    if sname:
                        groups[job_key]["status_names"].append(sname)
                    continue

                groups[job_key]["_seen"].add(dedupe_key)
                sname = status_map.get(r.get('status_id')) or ""
                if sname:
                    groups[job_key]["status_names"].append(sname)

                groups[job_key]["items"].append({
                    "sn": sn_val,
                    "title": r.get('sub_job_description') or "",
                    "comments": r.get('comment') or "",
                    "sub_job_id": sub_id if sub_id is not None else None
                })

            # finalize groups -> list, choose most common status_name per group (or blank)
            out = []
            for k in groups.keys():
                grp = groups[k]
                status_name_out = ""
                if grp["status_names"]:
                    cnt = Counter([s for s in grp["status_names"] if s])
                    if cnt:
                        status_name_out = cnt.most_common(1)[0][0] or ""
                out.append({
                    "job_id": grp["job_id"],
                    "title": grp["title"],
                    "status_name": status_name_out or "",
                    "items": grp["items"]
                })

            return success_resp("To-do list fetched", out, 200)
    finally:
        conn.close()


# ----------------------------
# DELETE TO-DO ITEM
# ----------------------------
@router.delete("/delete/{to_do_id}")
def delete_to_do_item(to_do_id: int):
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT 1 FROM to_do_list WHERE id=%s", (to_do_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail=f"To-do item {to_do_id} not found")
            
            cursor.execute("DELETE FROM to_do_list WHERE id=%s", (to_do_id,))
            conn.commit()
            return success_resp("To-do item deleted", {"id": to_do_id}, 200)
    finally:
        conn.close()



@router.get("/flagged/inspection/{inspection_id}/grouped")
def get_flagged_by_inspection_grouped(inspection_id: int):
    """
    Return flagged items for an inspection grouped into sections to match the
    'inspection checklist' JSON format the client expects.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT * FROM to_do_list WHERE inspection_id=%s ORDER BY created_at DESC", (inspection_id,))
            rows = cursor.fetchall() or []
            if not rows:
                return success_resp("Flagged items fetched", {"inspection_id": str(inspection_id), "tank_id": "", "emp_id": "", "sections": []}, 200)

            sections = {}
            tank_id = rows[0].get("tank_id")
            emp_id = None
            checklist_ids = [r["checklist_id"] for r in rows if r.get("checklist_id")]
            checklist_map = {}
            if checklist_ids:
                fmt = ','.join(['%s'] * len(checklist_ids))
                cursor.execute(f"SELECT id, job_id, sub_job_id, emp_id FROM inspection_checklist WHERE id IN ({fmt})", tuple(checklist_ids))
                chk_rows = cursor.fetchall() or []
                for cr in chk_rows:
                    checklist_map[cr.get('id')] = cr
                    if emp_id is None and cr.get('emp_id') is not None:
                        emp_id = cr.get('emp_id')

            for r in rows:
                chk_id = r.get('checklist_id')
                job_id = None
                sub_id = None
                if chk_id and chk_id in checklist_map:
                    job_id = checklist_map[chk_id].get('job_id')
                    sub_id = checklist_map[chk_id].get('sub_job_id')
                    if emp_id is None:
                        emp_id = checklist_map[chk_id].get('emp_id')

                job_key = str(job_id) if job_id is not None else ""
                if job_key not in sections:
                    sections[job_key] = {"job_id": job_key, "title": r.get('job_name'), "status_name": str(r.get('status_id') or ""), "items": []}

                sections[job_key]["items"].append({
                    "sn": r.get('sn') or "",
                    "title": r.get('sub_job_description'),
                    "job_id": job_key,
                    "sub_job_id": str(sub_id or ""),
                })

            # Convert status_id in sections to readable status_name (inspection_status)
            try:
                cursor.execute("SELECT status_id, status_name FROM inspection_status")
                status_map = {r.get('status_id'): r.get('status_name') for r in (cursor.fetchall() or [])}
            except Exception:
                status_map = {}

            for sec in sections.values():
                try:
                    sid = sec.get('status_name')
                    if sid is not None and sid != "":
                        # sid might be numeric string – convert
                        try:
                            sid_int = int(sid)
                            sec['status_name'] = status_map.get(sid_int) or ""
                        except Exception:
                            # sid already textual
                            sec['status_name'] = sid
                except Exception:
                    sec['status_name'] = sec.get('status_name') or ""

            resp = {
                "inspection_id": str(inspection_id),
                "tank_id": str(tank_id) if tank_id is not None else "",
                "emp_id": str(emp_id) if emp_id is not None else "",
                "sections": list(sections.values()),
            }
            return success_resp("Flagged items fetched (grouped)", resp, 200)
    except Exception as e:
        logger.error(f"Error fetching grouped flagged items for inspection {inspection_id}: {e}", exc_info=True)
        return error_resp(str(e), 500)
    finally:
        conn.close()
