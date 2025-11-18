from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from pymysql.cursors import DictCursor
from app.database import get_db_connection
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tank_checkpoints", tags=["tank_checkpoints"])


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
    try:
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
    except Exception as e:
        logger.warning(f"Could not sync flagged item to to_do_list: {e}")


class ChecklistCreate(BaseModel):
    report_id: Optional[int] = None
    tank_number: Optional[str] = None
    job_id: int = Field(...)
    sub_job_id: int = Field(...)
    status: Optional[str] = None
    comment: Optional[str] = None
    photo_path: Optional[str] = None



class ChecklistUpdate(BaseModel):
    report_id: Optional[int] = None
    tank_number: Optional[str] = None
    job_id: int = Field(...)
    sub_job_id: int = Field(...)
    status: Optional[str] = None
    comment: Optional[str] = None
    photo_path: Optional[str] = None


class ChecklistDelete(BaseModel):
    tank_number: str = Field(...)
    job_id: int = Field(...)
    sub_job_id: int = Field(...)


@router.get("/jobs")
def get_jobs():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT job_id, job_code, job_description, sort_order, created_at, updated_at FROM inspection_job ORDER BY sort_order, job_id")
            rows = cursor.fetchall()
            return {"success": True, "data": rows}
    finally:
        conn.close()


@router.get("/sub-jobs")
def get_sub_jobs(job_id: Optional[int] = None):
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            if job_id:
                cursor.execute("SELECT sub_job_id, job_id, sn, sub_job_description, sort_order, created_at, updated_at FROM inspection_sub_job WHERE job_id=%s ORDER BY sort_order, sub_job_id", (job_id,))
            else:
                cursor.execute("SELECT sub_job_id, job_id, sn, sub_job_description, sort_order, created_at, updated_at FROM inspection_sub_job ORDER BY job_id, sort_order, sub_job_id")
            rows = cursor.fetchall()
            return {"success": True, "data": rows}
    finally:
        conn.close()


@router.get("/inspection-status")
def get_inspection_status():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT status_id, status_name, description, sort_order, created_at, updated_at FROM inspection_status ORDER BY sort_order, status_id")
            rows = cursor.fetchall()
            return {"success": True, "data": rows}
    finally:
        conn.close()


def _create_or_get_report(cursor, tank_number: str):
    # find report for today
    cursor.execute("SELECT id FROM inspection_report WHERE tank_number=%s AND inspection_date=CURDATE()", (tank_number,))
    r = cursor.fetchone()
    if r:
        return r["id"]
    # create minimal report
    cursor.execute("INSERT INTO inspection_report (tank_number, inspection_date, emp_id, notes) VALUES (%s, CURDATE(), NULL, NULL)", (tank_number,))
    return cursor.lastrowid


@router.post("/create/inspection_checklist")
def create_inspection_checklist(payload: ChecklistCreate):
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # If report_id is provided, use it directly; otherwise create/get report using tank_number
            report_id = payload.report_id
            tank_number = payload.tank_number
            
            if report_id:
                # Verify report exists and get tank_number from it
                cursor.execute("SELECT tank_number FROM inspection_report WHERE id=%s", (report_id,))
                report_row = cursor.fetchone()
                if not report_row:
                    raise HTTPException(status_code=400, detail="Report not found")
                tank_number = report_row.get("tank_number")
            elif tank_number:
                # Create or get report for today using tank_number
                report_id = _create_or_get_report(cursor, tank_number)
            else:
                raise HTTPException(status_code=400, detail="Either report_id or tank_number must be provided")

            # ensure tank exists in tank_header
            cursor.execute("SELECT 1 FROM tank_header WHERE tank_number=%s", (tank_number,))
            if not cursor.fetchone():
                raise HTTPException(status_code=400, detail="Tank not found")

            # ensure job and subjob exist and fetch their names
            cursor.execute("SELECT job_code, job_description FROM inspection_job WHERE job_id=%s", (payload.job_id,))
            job_row = cursor.fetchone()
            if not job_row:
                raise HTTPException(status_code=400, detail="Job not found")
            # prefer descriptive name, fall back to code
            job_name = job_row.get("job_description") or job_row.get("job_code")

            cursor.execute("SELECT sn, sub_job_description FROM inspection_sub_job WHERE sub_job_id=%s AND job_id=%s", (payload.sub_job_id, payload.job_id))
            sub = cursor.fetchone()
            if not sub:
                raise HTTPException(status_code=400, detail="Sub-job not found for given job")
            sn = sub["sn"]
            sub_job_description = sub.get("sub_job_description")

            # resolve status to status_id
            status_id = None
            status_name = None
            if payload.status is not None:
                # try numeric
                try:
                    sid = int(payload.status)
                    cursor.execute("SELECT status_name FROM inspection_status WHERE status_id=%s", (sid,))
                    row = cursor.fetchone()
                    if row:
                        status_id = sid
                        status_name = row["status_name"]
                    else:
                        raise HTTPException(status_code=400, detail="Invalid status id")
                except ValueError:
                    # treat as name
                    cursor.execute("SELECT status_id FROM inspection_status WHERE LOWER(status_name)=LOWER(%s)", (payload.status,))
                    row = cursor.fetchone()
                    if row:
                        status_id = row["status_id"]
                        status_name = payload.status
                    else:
                        raise HTTPException(status_code=400, detail="Invalid status name")

            # insert checklist row; use unique (report_id, sn)
            # compute flagged automatically: flagged if a non-empty comment was provided
            flagged_val = 1 if (payload.comment and str(payload.comment).strip() != "") else 0

            sql = """
                INSERT INTO inspection_checklist (report_id, tank_number, job_id, job_name, sn, sub_job_description, status_id, status, comment, photo_path, flagged, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON DUPLICATE KEY UPDATE job_id=VALUES(job_id), job_name=VALUES(job_name), sub_job_description=VALUES(sub_job_description), status_id=VALUES(status_id), status=VALUES(status), comment=VALUES(comment), photo_path=VALUES(photo_path), flagged=VALUES(flagged), updated_at=NOW()
            """
            cursor.execute(sql, (
                report_id,
                tank_number,
                payload.job_id,
                job_name,
                sn,
                sub_job_description,
                status_id,
                status_name,
                payload.comment,
                payload.photo_path,
                flagged_val,
            ))
            conn.commit()
            # return the inserted/updated row
            cursor.execute("SELECT id FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            checklist_row = cursor.fetchone()
            if checklist_row:
                checklist_id = checklist_row['id']
                # If flagged, sync to to_do_list
                if flagged_val == 1:
                    _sync_flagged_to_todo(cursor, checklist_id)
                    conn.commit()
            
            cursor.execute("SELECT * FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            row = cursor.fetchone()
            return {"success": True, "data": row}
    finally:
        conn.close()



@router.put("/update/inspection_checklist")
def update_inspection_checklist(payload: ChecklistUpdate):
    """Update an existing inspection_checklist row. Returns updated row."""
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            # If report_id is provided, use it; otherwise use tank_number to find today's report
            report_id = payload.report_id
            tank_number = payload.tank_number
            
            if report_id:
                # Verify report exists and get tank_number from it
                cursor.execute("SELECT tank_number FROM inspection_report WHERE id=%s", (report_id,))
                report_row = cursor.fetchone()
                if not report_row:
                    raise HTTPException(status_code=400, detail="Report not found")
                tank_number = report_row.get("tank_number")
            elif tank_number:
                # Find today's report using tank_number
                cursor.execute("SELECT id FROM inspection_report WHERE tank_number=%s AND inspection_date=CURDATE()", (tank_number,))
                r = cursor.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Inspection report for today not found")
                report_id = r["id"]
            else:
                raise HTTPException(status_code=400, detail="Either report_id or tank_number must be provided")

            # ensure tank exists
            cursor.execute("SELECT 1 FROM tank_header WHERE tank_number=%s", (tank_number,))
            if not cursor.fetchone():
                raise HTTPException(status_code=400, detail="Tank not found")

            # ensure job and subjob exist and fetch sn
            cursor.execute("SELECT sn FROM inspection_sub_job WHERE sub_job_id=%s AND job_id=%s", (payload.sub_job_id, payload.job_id))
            sub = cursor.fetchone()
            if not sub:
                raise HTTPException(status_code=400, detail="Sub-job not found for given job")
            sn = sub["sn"]

            # resolve status
            status_id = None
            status_name = None
            if payload.status is not None:
                try:
                    sid = int(payload.status)
                    cursor.execute("SELECT status_name FROM inspection_status WHERE status_id=%s", (sid,))
                    row = cursor.fetchone()
                    if row:
                        status_id = sid
                        status_name = row["status_name"] if isinstance(row, dict) else row[0]
                    else:
                        raise HTTPException(status_code=400, detail="Invalid status id")
                except ValueError:
                    cursor.execute("SELECT status_id FROM inspection_status WHERE LOWER(status_name)=LOWER(%s)", (payload.status,))
                    row = cursor.fetchone()
                    if row:
                        status_id = row["status_id"]
                        status_name = payload.status
                    else:
                        raise HTTPException(status_code=400, detail="Invalid status name")

            # check existing row
            cursor.execute("SELECT 1 FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Checklist row not found")

            flagged_val = 1 if (payload.comment and str(payload.comment).strip() != "") else 0

            cursor.execute("""
                UPDATE inspection_checklist
                SET status_id=%s, status=%s, comment=%s, photo_path=%s, flagged=%s, updated_at=NOW()
                WHERE report_id=%s AND sn=%s
            """, (
                status_id,
                status_name,
                payload.comment,
                payload.photo_path,
                flagged_val,
                report_id,
                sn
            ))
            conn.commit()
            
            # Fetch updated row and sync to to_do_list if flagged
            cursor.execute("SELECT id FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            checklist_row = cursor.fetchone()
            if checklist_row:
                checklist_id = checklist_row['id']
                if flagged_val == 1:
                    _sync_flagged_to_todo(cursor, checklist_id)
                    conn.commit()
                else:
                    # If no longer flagged, remove from to_do_list
                    try:
                        cursor.execute("DELETE FROM to_do_list WHERE checklist_id=%s", (checklist_id,))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Could not delete from to_do_list: {e}")
            
            cursor.execute("SELECT * FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            row = cursor.fetchone()
            return {"success": True, "data": row}
    finally:
        conn.close()


@router.delete("/delete/inspection_checklist")
def delete_inspection_checklist(payload: ChecklistDelete):
    """Delete an inspection_checklist row for today's report identified by tank_number + job_id + sub_job_id."""
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT sn FROM inspection_sub_job WHERE sub_job_id=%s AND job_id=%s", (payload.sub_job_id, payload.job_id))
            sub = cursor.fetchone()
            if not sub:
                raise HTTPException(status_code=400, detail="Sub-job not found for given job")
            sn = sub["sn"]

            cursor.execute("SELECT id FROM inspection_report WHERE tank_number=%s AND inspection_date=CURDATE()", (payload.tank_number,))
            r = cursor.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Inspection report for today not found")
            report_id = r["id"]

            cursor.execute("SELECT 1 FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Checklist row not found")

            # Get checklist_id before deleting
            cursor.execute("SELECT id FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            checklist_row = cursor.fetchone()
            checklist_id = checklist_row['id'] if checklist_row else None

            cursor.execute("DELETE FROM inspection_checklist WHERE report_id=%s AND sn=%s", (report_id, sn))
            
            # Also remove from to_do_list if it exists
            if checklist_id:
                try:
                    cursor.execute("DELETE FROM to_do_list WHERE checklist_id=%s", (checklist_id,))
                except Exception as e:
                    logger.warning(f"Could not delete from to_do_list: {e}")
            
            conn.commit()
            return {"success": True, "data": {"report_id": report_id, "sn": sn}}
    finally:
        conn.close()
