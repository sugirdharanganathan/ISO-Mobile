from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel, Field
from typing import Optional, List
from pymysql.cursors import DictCursor
from app.database import get_db_connection, get_db
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from app.models.inspection_checklist_model import InspectionChecklist
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


class ChecklistUpdate(BaseModel):
    report_id: Optional[int] = None
    tank_number: Optional[str] = None
    job_id: int = Field(...)
    sub_job_id: int = Field(...)
    status: Optional[str] = None
    comment: Optional[str] = None



class ChecklistDelete(BaseModel):
    report_id: Optional[int] = None
    tank_number: str = Field(...)
    job_id: int = Field(...)
    sub_job_id: int = Field(...)


EXAMPLE_INSPECTION_CHECKLIST = {
  "report_id": 1,
  "tank_number": "string",
  "sections": [
    {
      "job_id": 1,
      "job_title": "Tank Body & Frame Condition",
      "items": [
        {
          "sub_job_id": 1,
          "sn": "1.1",
          "title": "Body x 6 Sides & All Frame – No Dent / No Bent / No Deep Cut",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 2,
          "sn": "1.2",
          "title": "Cabin Door & Frame Condition – No Damage / Can Lock",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 3,
          "sn": "1.3",
          "title": "Tank Number, Product & Hazchem Label – Not Missing or Tear",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 4,
          "sn": "1.4",
          "title": "Condition of Paint Work & Cleanliness – Clean / No Bad Rust",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 5,
          "sn": "1.5",
          "title": "Others",
          "status": "",
          "comment": ""
        }
      ]
    },
    {
      "job_id": 2,
      "job_title": "Pipework & Installation",
      "items": [
        {
          "sub_job_id": 1,
          "sn": "2.1",
          "title": "Pipework Supports / Brackets – Not Loose / No Bent",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 2,
          "sn": "2.2",
          "title": "Pipework Joint & Welding – No Crack / No Icing / No Leaking",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 3,
          "sn": "2.3",
          "title": "Earthing Point",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 4,
          "sn": "2.4",
          "title": "PBU Support & Flange Connection – No Leak / Not Damage",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 5,
          "sn": "2.5",
          "title": "Others",
          "status": "",
          "comment": ""
        }
      ]
    },
    {
      "job_id": 3,
      "job_title": "Tank Instrument & Assembly",
      "items": [
        {
          "sub_job_id": 1,
          "sn": "3.1",
          "title": "Safety Diverter Valve – Switching Lever",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 2,
          "sn": "3.2",
          "title": "Safety Valves Connection & Joint – No Leaks",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 3,
          "sn": "3.3",
          "title": "Level & Pressure Gauge Support Bracket, Connection & Joint – Not Loosen / No Leaks",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 4,
          "sn": "3.4",
          "title": "Level & Pressure Gauge – Function Check",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 5,
          "sn": "3.5",
          "title": "Level & Pressure Gauge Valve Open / Balance Valve Close",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 6,
          "sn": "3.6",
          "title": "Data & CSC Plate – Not Missing / Not Damage",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 7,
          "sn": "3.7",
          "title": "Others",
          "status": "",
          "comment": ""
        }
      ]
    },
    {
      "job_id": 4,
      "job_title": "Valves Tightness & Operation",
      "items": [
        {
          "sub_job_id": 1,
          "sn": "4.1",
          "title": "Valve Handwheel – Not Missing / Nut Not Loose",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 2,
          "sn": "4.2",
          "title": "Valve Open & Close Operation – No Seizing / Not Tight / Not Jam",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 3,
          "sn": "4.3",
          "title": "Valve Tightness Incl Glands – No Leak / No Icing / No Passing",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 4,
          "sn": "4.4",
          "title": "Anchor Point",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 5,
          "sn": "4.5",
          "title": "Others",
          "status": "",
          "comment": ""
        }
      ]
    },
    {
      "job_id": 5,
      "job_title": "Before Departure Check",
      "items": [
        {
          "sub_job_id": 1,
          "sn": "5.1",
          "title": "All Valves Closed – Defrost & Close Firmly",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 2,
          "sn": "5.2",
          "title": "Caps fitted to Outlets or Cover from Dust if applicable",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 3,
          "sn": "5.3",
          "title": "Security Seal Fitted by Refilling Plant - Check",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 4,
          "sn": "5.4",
          "title": "Pressure Gauge – lowest possible",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 5,
          "sn": "5.5",
          "title": "Level Gauge – Within marking or standard indication",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 6,
          "sn": "5.6",
          "title": "Weight Reading – ensure within acceptance weight",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 7,
          "sn": "5.7",
          "title": "Cabin Door Lock – Secure and prevent from sudden opening",
          "status": "",
          "comment": ""
        },
        {
          "sub_job_id": 8,
          "sn": "5.8",
          "title": "Others",
          "status": "",
          "comment": ""
        }
      ]
    },
    {
      "job_id": 6,
      "job_title": "Others Observation & Comment",
      "items": []
    }
  ]
}


class FullChecklistItem(BaseModel):
    sub_job_id: int
    sn: str
    title: str
    status: Optional[str] = ""
    comment: Optional[str] = ""
    # for "Others" rows that have an extra items: [] field – we just ignore it
    items: Optional[list] = None


class FullChecklistSection(BaseModel):
    job_id: int
    job_title: str
    items: List[FullChecklistItem]


class FullInspectionChecklistCreate(BaseModel):
    report_id: Optional[int] = None
    tank_number: str
    sections: List[FullChecklistSection]

    class Config:
        json_schema_extra = {
            "example": EXAMPLE_INSPECTION_CHECKLIST
        }


@router.post("/create/inspection_checklist_bulk")
def create_inspection_checklist_bulk(
    payload: FullInspectionChecklistCreate,
    db: Session = Depends(get_db),
):
    """
    Create multiple inspection_checklist rows in a single transaction.

    - Accepts the nested `tank_number + sections[] + items[]` payload
    - Resolves job and sub-job from DB
    - Uses `sn` (e.g. "2.1") + job_id to find the correct sub_job row
    - Saves each item into inspection_checklist
    """
    from app.models.inspection_checklist_model import InspectionChecklist

    created_entries = []

    report_id = payload.report_id
    tank_number = payload.tank_number

    try:
        with db.begin():
            # --- resolve / create report_id ---
            if report_id:
                r = db.execute(
                    text(
                        "SELECT id, tank_number "
                        "FROM inspection_report WHERE id = :rid"
                    ),
                    {"rid": report_id},
                ).fetchone()
                if not r:
                    raise HTTPException(status_code=400, detail="Report not found")
                if not tank_number:
                    tank_number = r["tank_number"] if hasattr(r, "keys") else r[1]

            if not report_id:
                if not tank_number:
                    raise HTTPException(
                        status_code=400,
                        detail="Either report_id or tank_number must be provided",
                    )
                rr = db.execute(
                    text(
                        "SELECT id FROM inspection_report "
                        "WHERE tank_number = :tn AND inspection_date = CURDATE()"
                    ),
                    {"tn": tank_number},
                ).fetchone()
                if rr:
                    report_id = rr[0] if not hasattr(rr, "keys") else rr["id"]
                else:
                    db.execute(
                        text(
                            "INSERT INTO inspection_report "
                            "(tank_number, inspection_date, emp_id, notes, "
                            " created_at, updated_at) "
                            "VALUES (:tn, CURDATE(), NULL, NULL, NOW(), NOW())"
                        ),
                        {"tn": tank_number},
                    )
                    rr2 = db.execute(
                        text(
                            "SELECT id FROM inspection_report "
                            "WHERE tank_number = :tn AND inspection_date = CURDATE()"
                        ),
                        {"tn": tank_number},
                    ).fetchone()
                    if not rr2:
                        raise HTTPException(
                            status_code=500,
                            detail="Could not create inspection report",
                        )
                    report_id = rr2[0] if not hasattr(rr2, "keys") else rr2["id"]

            # --- ensure tank exists ---
            tcheck = db.execute(
                text("SELECT 1 FROM tank_header WHERE tank_number = :tn"),
                {"tn": tank_number},
            ).fetchone()
            if not tcheck:
                raise HTTPException(status_code=400, detail="Tank not found")

            # --- iterate sections/items ---
            to_insert = []

            for section in payload.sections:
                # job lookup
                job_row = db.execute(
                    text(
                        "SELECT job_code, job_description "
                        "FROM inspection_job WHERE job_id = :jid"
                    ),
                    {"jid": section.job_id},
                ).fetchone()
                if not job_row:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Job not found: {section.job_id}",
                    )

                job_name = (
                    job_row["job_description"]
                    if hasattr(job_row, "keys")
                    else job_row[1]
                ) or (job_row["job_code"] if hasattr(job_row, "keys") else job_row[0])

                for item in section.items:
                    # 🔴 key change: resolve sub-job by (job_id, sn), not by sub_job_id from payload
                    sub_row = db.execute(
                        
                        text(
                            "SELECT sub_job_id, sn, sub_job_description "
                            "FROM inspection_sub_job "
                            "WHERE job_id = :jid AND sn = :sn"
                        ),
                        {"jid": section.job_id, "sn": item.sn},
                    ).fetchone()

                    if not sub_row:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Sub-job {item.sn} not found for job {section.job_id}",
                        )

                    # real DB id for this sub-job (uses sub_job_id column)
                    db_sub_job_id = (
                        sub_row["sub_job_id"] if hasattr(sub_row, "keys") else sub_row[0]
                    )
                    db_sn = sub_row["sn"] if hasattr(sub_row, "keys") else sub_row[1]
                    sub_desc = (
                        sub_row["sub_job_description"]
                        if hasattr(sub_row, "keys")
                        else sub_row[2]
                    )


                    # status -> (status_id, status_name)
                    status_id = None
                    status_name = None
                    if item.status is not None:
                        try:
                            sid_int = int(item.status)
                            srow = db.execute(
                                text(
                                    "SELECT status_name FROM inspection_status "
                                    "WHERE status_id = :sid"
                                ),
                                {"sid": sid_int},
                            ).fetchone()
                            if srow:
                                status_id = sid_int
                                status_name = (
                                    srow["status_name"]
                                    if hasattr(srow, "keys")
                                    else srow[0]
                                )
                            else:
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Invalid status id: {item.status}",
                                )
                        except ValueError:
                            srow = db.execute(
                                text(
                                    "SELECT status_id FROM inspection_status "
                                    "WHERE LOWER(status_name) = LOWER(:sname)"
                                ),
                                {"sname": item.status},
                            ).fetchone()
                            if srow:
                                status_id = (
                                    srow["status_id"]
                                    if hasattr(srow, "keys")
                                    else srow[0]
                                )
                                status_name = item.status
                            else:
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Invalid status name: {item.status}",
                                )

                    flagged_val = bool(
                        item.comment and str(item.comment).strip() != ""
                    )

                    chk = InspectionChecklist(
                        report_id=report_id,
                        tank_number=tank_number,
                        job_id=section.job_id,
                        job_name=job_name,
                        sn=db_sn,
                        sub_job_id=db_sub_job_id,
                        sub_job_description=sub_desc,
                        status_id=status_id,
                        status=(
                            status_name
                            if status_name is not None
                            else (item.status if item.status is not None else None)
                        ),
                        comment=item.comment,
                        flagged=flagged_val,
                    )
                    db.add(chk)
                    to_insert.append((report_id, db_sn))

            db.flush()

            items_out = []
            for rep_id, sn in to_insert:
                row = db.execute(
                    text(
                        "SELECT * FROM inspection_checklist "
                        "WHERE report_id = :rid AND sn = :sn"
                    ),
                    {"rid": rep_id, "sn": sn},
                ).mappings().fetchone()
                if row:
                    items_out.append(dict(row))

        return {
            "report_id": report_id,
            "tank_number": tank_number,
            "total_items_created": len(items_out),
            "items": items_out,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
                SET status_id=%s, status=%s, comment=%s, flagged=%s, updated_at=NOW()
                WHERE report_id=%s AND sn=%s
            """, (
                status_id,
                status_name,
                payload.comment,
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
