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

class ToDoBulkItem(BaseModel):
    id: int             # To-Do ID
    status_id: int
    comment: Optional[str] = None


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
                    groups[job_key] = {"job_id": job_id_out, "title": title, "status_ids": [], "items": [], "_seen": set()}

                # dedupe by (sub_job_id, sn)
                sn_val = r.get('sn') or ""
                dedupe_key = (None if sub_id is None else (int(sub_id) if str(sub_id).isdigit() else str(sub_id)), str(sn_val))
                if dedupe_key in groups[job_key]["_seen"]:
                    # collect status_id for later aggregation
                    sid_value = r.get('status_id')
                    if sid_value is not None:
                        groups[job_key]["status_ids"].append(sid_value)
                    continue

                groups[job_key]["_seen"].add(dedupe_key)
                sid_value = r.get('status_id')
                if sid_value is not None:
                    groups[job_key]["status_ids"].append(sid_value)

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
                status_id_out = ""
                if grp["status_ids"]:
                    cnt = Counter([s for s in grp["status_ids"] if s is not None])
                    if cnt:
                        most_common = cnt.most_common(1)[0][0]
                        status_id_out = str(most_common) if most_common is not None else ""
                out.append({
                    "job_id": grp["job_id"],
                    "title": grp["title"],
                    "status_id": status_id_out or "",
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
    Return flagged items for an inspection grouped into sections.
    Uses SQL JOINs to ensure job_id and sub_job_id are retrieved correctly.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # 1. Fetch To-Do Items with JOIN to get checklist details (job_id, sub_job_id)
            query = """
                SELECT 
                    t.sn, 
                    t.sub_job_description as title,
                    t.status_id,
                    t.checklist_id,
                    t.tank_id,
                    t.job_name,
                    c.job_id, 
                    c.sub_job_id, 
                    c.emp_id as checklist_emp_id
                FROM to_do_list t
                LEFT JOIN inspection_checklist c ON t.checklist_id = c.id
                WHERE t.inspection_id = %s
                ORDER BY t.created_at DESC
            """
            cursor.execute(query, (inspection_id,))
            rows = cursor.fetchall() or []
            
            # 2. Get Header Info (Fallback for emp_id)
            # We fetch emp_id from the header in case the checklist item is missing it
            cursor.execute("SELECT emp_id, tank_id FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1", (inspection_id,))
            header = cursor.fetchone()
            
            header_emp_id = str(header.get("emp_id")) if header and header.get("emp_id") else ""
            header_tank_id = str(header.get("tank_id")) if header and header.get("tank_id") else ""

            # 3. Group the items
            sections = {}
            
            for r in rows:
                # Resolve IDs (prefer from checklist join, fallback to empty)
                job_id = r.get('job_id')
                sub_id = r.get('sub_job_id')
                
                # Convert to string, defaulting to "" if None
                job_key = str(job_id) if job_id is not None else ""
                sub_key = str(sub_id) if sub_id is not None else ""
                
                # If job_id is missing (broken link), try to use job_name as title
                section_title = r.get('job_name') or "Unknown Section"

                # Initialize section if not exists
                if job_key not in sections:
                    sections[job_key] = {
                        "job_id": job_key, 
                        "title": section_title, 
                        "status_id": str(r.get('status_id') or ""), 
                        "items": []
                    }

                sections[job_key]["items"].append({
                    "sn": r.get('sn') or "",
                    "title": r.get('title'),
                    "job_id": job_key,
                    "sub_job_id": sub_key,
                })

            # 4. Final Response Construction
            # Use header emp_id if we have it, otherwise try to find one from the rows
            final_emp_id = header_emp_id
            if not final_emp_id and rows:
                # Try to grab from first row's checklist join
                if rows[0].get('checklist_emp_id'):
                    final_emp_id = str(rows[0].get('checklist_emp_id'))

            resp = {
                "inspection_id": str(inspection_id),
                "tank_id": header_tank_id if header_tank_id else (str(rows[0].get('tank_id')) if rows else ""),
                "emp_id": final_emp_id,
                "sections": list(sections.values()),
            }
            return success_resp("Flagged items fetched (grouped)", resp, 200)

    except Exception as e:
        logger.error(f"Error fetching grouped flagged items for inspection {inspection_id}: {e}", exc_info=True)
        return error_resp(str(e), 500)
    finally:
        conn.close()

# ----------------------------
# UPDATE TO-DO ITEM (SYNC BACK)
# ----------------------------
from fastapi import Header
from typing import Union

class ToDoJobUpdate(BaseModel):
    job_id: Union[int, str]
    status_id: int
    comment: Optional[str] = None


@router.put("/update")
def update_to_do_by_inspection(
    items: List[ToDoJobUpdate],
    Authorization: Optional[str] = Header(None),
    Inspection_Id: Optional[Union[int, str]] = Header(None, alias="inspection_id"),
):
    """
    Update To-Do items by inspection_id and job_id.
    
    GET behavior (when items list is empty or method is GET):
    - Returns all job_ids and their status_ids present in to_do_list for the inspection_id
    
    PUT behavior (when items list contains updates):
    - If status_id changes from 2 to 1 or 2 to 3, removes ALL items with that job_id from to_do_list
    - Updates ALL inspection_checklist rows with that job_id to the new status_id
    - Removes items from to_do_list for that job_id
    
    Authorization header is required.
    """
    if not Authorization:
        return error_resp("Authorization required", 401)
    
    if Inspection_Id is None or str(Inspection_Id).strip() == "":
        return error_resp("inspection_id header required", 400)
    
    try:
        inspection_id = int(str(Inspection_Id))
    except Exception:
        return error_resp("Invalid inspection_id header value", 400)
    
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # If no items provided, return current state (GET-like behavior)
            if not items or len(items) == 0:
                cursor.execute("""
                    SELECT DISTINCT t.job_name, c.job_id, t.status_id
                    FROM to_do_list t
                    LEFT JOIN inspection_checklist c ON t.checklist_id = c.id
                    WHERE t.inspection_id = %s
                    ORDER BY c.job_id
                """, (inspection_id,))
                rows = cursor.fetchall() or []
                
                result = []
                for r in rows:
                    result.append({
                        "job_id": str(r.get('job_id')) if r.get('job_id') is not None else "",
                        "job_name": r.get('job_name') or "",
                        "status_id": str(r.get('status_id')) if r.get('status_id') is not None else ""
                    })
                
                return success_resp("To-do items for inspection fetched", {
                    "inspection_id": str(inspection_id),
                    "jobs": result
                }, 200)
            
            # UPDATE behavior - process each job_id update
            updated_jobs = []
            for item in items:
                job_id = item.job_id
                new_status_id = item.status_id
                comment = item.comment
                
                # Find all checklist_ids for this job_id in to_do_list for this inspection
                cursor.execute("""
                    SELECT t.id as todo_id, t.checklist_id, c.job_id
                    FROM to_do_list t
                    LEFT JOIN inspection_checklist c ON t.checklist_id = c.id
                    WHERE t.inspection_id = %s AND c.job_id = %s
                """, (inspection_id, job_id))
                
                todo_rows = cursor.fetchall() or []
                
                if not todo_rows:
                    continue
                
                # Update all inspection_checklist rows for this job_id and inspection_id
                # Also fetch and update status name
                status_name = None
                try:
                    cursor.execute("SELECT status FROM inspection_status WHERE id = %s LIMIT 1", (new_status_id,))
                    status_row = cursor.fetchone()
                    if status_row:
                        status_name = status_row.get('status')
                except Exception:
                    pass
                
                # Determine if this status should be flagged (only status_id=2 is flagged)
                new_flagged = 1 if new_status_id == 2 else 0
                
                # Update all checklist items for this job_id
                update_sql = """
                    UPDATE inspection_checklist 
                    SET status_id=%s, flagged=%s, updated_at=NOW()
                """
                update_params = [new_status_id, new_flagged]
                
                if status_name:
                    update_sql += ", status=%s"
                    update_params.append(status_name)
                
                if comment is not None:
                    update_sql += ", comment=%s"
                    update_params.append(comment)
                
                update_sql += " WHERE inspection_id=%s AND job_id=%s"
                update_params.extend([inspection_id, job_id])
                
                cursor.execute(update_sql, tuple(update_params))
                
                # If status changed from 2 to something else (1 or 3), remove from to_do_list
                if new_status_id != 2:
                    # Delete all to_do_list entries for this job_id and inspection_id
                    for todo_row in todo_rows:
                        cursor.execute("DELETE FROM to_do_list WHERE id=%s", (todo_row['todo_id'],))
                else:
                    # Status is still 2 (flagged), update to_do_list entries
                    for todo_row in todo_rows:
                        update_todo_sql = "UPDATE to_do_list SET status_id=%s"
                        update_todo_params = [new_status_id]
                        
                        if comment is not None:
                            update_todo_sql += ", comment=%s"
                            update_todo_params.append(comment)
                        
                        update_todo_sql += " WHERE id=%s"
                        update_todo_params.append(todo_row['todo_id'])
                        
                        cursor.execute(update_todo_sql, tuple(update_todo_params))
                
                updated_jobs.append({
                    "job_id": str(job_id),
                    "status_id": str(new_status_id),
                    "removed_from_todo": new_status_id != 2
                })
            
            conn.commit()
            return success_resp(f"Successfully updated {len(updated_jobs)} job(s)", {
                "inspection_id": str(inspection_id),
                "updated_jobs": updated_jobs
            }, 200)
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating to-do by inspection: {e}", exc_info=True)
        return error_resp(str(e), 500)
    finally:
        conn.close()
