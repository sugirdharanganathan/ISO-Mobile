from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from app.database import get_db_connection, get_db
from app.models.to_do_list_model import ToDoList
from pymysql.cursors import DictCursor
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/to_do_list", tags=["to_do_list"])


# ----------------------------
# RESPONSE MODELS
# ----------------------------
class ToDoListResponse(BaseModel):
    id: int
    checklist_id: int
    inspection_id: int
    tank_number: str
    job_name: Optional[str]
    sub_job_description: Optional[str]
    sn: str
    status_id: Optional[int]
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
        SELECT id, inspection_id, tank_number, job_name, sub_job_description, sn, status_id, comment, created_at
        FROM inspection_checklist
        WHERE id=%s AND flagged=1
    """, (checklist_id,))
    row = cursor.fetchone()
    if not row:
        return
    cursor.execute("""
        INSERT INTO to_do_list (checklist_id, inspection_id, tank_number, job_name, sub_job_description, sn, status_id, comment, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            inspection_id=VALUES(inspection_id),
            tank_number=VALUES(tank_number),
            job_name=VALUES(job_name),
            sub_job_description=VALUES(sub_job_description),
            status_id=VALUES(status_id),
            comment=VALUES(comment)
    """, (
        checklist_id,
        row['inspection_id'],
        row['tank_number'],
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
                SELECT id, checklist_id, inspection_id, tank_number, job_name, sub_job_description,
                       sn, status_id, comment, created_at
                FROM to_do_list
                ORDER BY created_at DESC
            """)
            rows = cursor.fetchall()
            return {"success": True, "data": rows}
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
            return {"success": True, "data": {"id": to_do_id}}
    finally:
        conn.close()


# ----------------------------
# GET FLAGGED ITEMS BY INSPECTION_ID
# ----------------------------
@router.get("/flagged/inspection/{inspection_id}")
def get_flagged_by_inspection(inspection_id: int):
    """
    Fetch all flagged items for a specific inspection_id from to_do_list.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("""
                SELECT id, checklist_id, inspection_id, tank_number, job_name, sub_job_description,
                       sn, status_id, comment, created_at
                FROM to_do_list
                WHERE inspection_id=%s
                ORDER BY created_at DESC
            """, (inspection_id,))
            
            rows = cursor.fetchall()
            return {"success": True, "data": rows}
    except Exception as e:
        logger.error(f"Error fetching flagged items for inspection {inspection_id}: {e}", exc_info=True)
        return {"success": False, "data": [], "error": str(e)}
    finally:
        conn.close()
