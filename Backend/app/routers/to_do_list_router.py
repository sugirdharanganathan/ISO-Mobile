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


class ToDoListResponse(BaseModel):
    id: int
    checklist_id: int
    report_id: int
    tank_number: str
    job_name: Optional[str]
    sub_job_description: Optional[str]
    sn: str
    status: Optional[str]
    comment: Optional[str]
    created_at: str


class GenericResponse(BaseModel):
    success: bool
    data: List[dict]


def _sync_flagged_to_todo(cursor, checklist_id: int):
    """
    Helper to sync a flagged checklist item to to_do_list table.
    Fetches the checklist row and inserts/updates to to_do_list if flagged=1.
    """
    # Get the flagged checklist row
    cursor.execute("""
        SELECT id, report_id, tank_number, job_name, sub_job_description, sn, status, comment, created_at
        FROM inspection_checklist
        WHERE id=%s AND flagged=1
    """, (checklist_id,))
    
    row = cursor.fetchone()
    if not row:
        return  # Not flagged or doesn't exist
    
    # Insert or update in to_do_list
    cursor.execute("""
        INSERT INTO to_do_list (checklist_id, report_id, tank_number, job_name, sub_job_description, sn, status, comment, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            tank_number=VALUES(tank_number),
            job_name=VALUES(job_name),
            sub_job_description=VALUES(sub_job_description),
            status=VALUES(status),
            comment=VALUES(comment)
    """, (
        checklist_id,
        row['report_id'],
        row['tank_number'],
        row['job_name'],
        row['sub_job_description'],
        row['sn'],
        row['status'],
        row['comment'],
        row['created_at']
    ))


@router.get("/list", response_model=GenericResponse)
def get_to_do_list():
    """Get all flagged items from to_do_list table"""
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("""
                SELECT id, checklist_id, report_id, tank_number, job_name, sub_job_description, 
                       sn, status, comment, created_at
                FROM to_do_list
                ORDER BY created_at DESC
            """)
            rows = cursor.fetchall()
            return {"success": True, "data": rows}
    finally:
        conn.close()


@router.delete("/delete/{to_do_id}")
def delete_to_do_item(to_do_id: int):
    """Delete a to_do_list item by ID"""
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # Check if exists
            cursor.execute("SELECT 1 FROM to_do_list WHERE id=%s", (to_do_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail=f"To-do item {to_do_id} not found")
            
            # Delete from to_do_list
            cursor.execute("DELETE FROM to_do_list WHERE id=%s", (to_do_id,))
            conn.commit()
            
            return {"success": True, "data": {"id": to_do_id}}
    finally:
        conn.close()


@router.get("/checklist/{checklist_id}")
def get_to_do_by_checklist(checklist_id: int):
    """Get to_do_list item by checklist_id"""
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("""
                SELECT id, checklist_id, report_id, tank_number, job_name, sub_job_description,
                       sn, status, comment, created_at
                FROM to_do_list
                WHERE checklist_id=%s
            """, (checklist_id,))
            
            row = cursor.fetchone()
            if not row:
                return {"success": False, "data": None}
            
            return {"success": True, "data": row}
    finally:
        conn.close()
