from fastapi import APIRouter, Depends, Header, status
from fastapi.responses import JSONResponse
from typing import Optional
import logging
from app.database import get_db, get_db_connection
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.routers.tank_inspection_router import get_current_user
import re

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/validation", tags=["validation"])


def _error(message: str = "Error", status_code: int = 400):
    return JSONResponse(status_code=status_code, content={"success": False, "message": message, "data": {}})


def _success(data=None, message: str = "Operation successful"):
    return JSONResponse(status_code=200, content={"success": True, "message": message, "data": data or {}})


@router.get("/inspection/{inspection_id}")
def validate_inspection(
    inspection_id: int,
    Authorization: Optional[str] = Header(None),
    current_user: Optional[dict] = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Validate that for the given inspection_id:
    - tank_inspection_details: required fields are non-null (except operator_id, safety_valve_model_id, safety_valve_size_id allowed to be 0/null)
    - inspection_checklist: items for the inspection contain job_id/sub_job_id/sn/status_id
    - tank_images: at least 15 images uploaded; each image has image_path and image_type
    Returns lists of missing fields and any per-row issues found.
    """
    if current_user is None:
        return _error("Authorization required", status_code=status.HTTP_401_UNAUTHORIZED)

    # 1) Check inspection row
    issues = {"inspection": [], "checklist": [], "to_do_list": [], "images": []}
    try:
        row = db.execute(text("SELECT * FROM tank_inspection_details WHERE inspection_id = :id LIMIT 1"), {"id": inspection_id}).fetchone()
        if not row:
            return _error(f"Inspection {inspection_id} not found", status_code=404)

        # convert to mapping/dict
        if hasattr(row, "_mapping"):
            insp = dict(row._mapping)
        elif isinstance(row, dict):
            insp = row
        else:
            try:
                insp = dict(zip(row.keys(), row))
            except Exception:
                insp = {}

        # Required fields to check (non-null and non-empty): This is a practical set
        required_inspection_fields = [
            "tank_id", "tank_number", "report_number", "inspection_date",
            "status_id", "product_id", "inspection_type_id", "location_id",
            # pi_next_inspection_date will be validated separately (it can exist under several names)
        ]

        for f in required_inspection_fields:
            v = insp.get(f)
            if v is None or (isinstance(v, str) and v.strip() == ""):
                issues["inspection"].append({"field": f, "reason": "null or empty"})
            else:
                # If numeric check: value shouldn't be 0 (except allowed columns)
                if isinstance(v, (int, float)) and int(v) == 0:
                    issues["inspection"].append({"field": f, "reason": "zero or invalid"})

        # operator_id, safety_valve_model_id and safety_valve_size_id are allowed to be 0
        # Other fields like lifter_weight etc. are optional; we do not enforce them here

    except Exception as e:
        logger.exception("Error validating inspection: %s", e)
        return _error(f"Error validating inspection: {e}", status_code=500)

    # validate PI next inspection date (several column name variants may exist)
    try:
        pi_keys = ["pi_next_inspection_date", "pi_next_insp_date", "next_insp_date", "pi_nextinsp_date"]
        pi_found = False
        for k in pi_keys:
            v = insp.get(k)
            if v is not None and not (isinstance(v, str) and v.strip() == ""):
                pi_found = True
                break
        if not pi_found:
            issues["inspection"].append({"field": "pi_next_inspection_date", "reason": "null or empty (or alternate name missing)"})
    except Exception:
        pass

    # 2) Validate inspection_checklist (items exist and fields present)
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
        logger.exception("Error validating checklist for inspection %s: %s", inspection_id, e)
        return _error(f"Error validating checklist: {e}", status_code=500)

    # 2.5) Validate to_do_list is empty for this inspection
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
        logger.exception("Error validating to_do_list for inspection %s: %s", inspection_id, e)
        # Don't fail the whole validation, just log the error

    # Helper: normalize names (strip non-alpha, lower)
    def _norm_name(s):
        if s is None:
            return None
        try:
            s2 = str(s).strip().lower()
        except Exception:
            s2 = str(s)
        # Keep only letters a-z to normalize common variants e.g. 'Underside View 01' -> 'undersideview'
        return re.sub('[^a-z]', '', s2)

    # 3) Validate images: check count >= expected and expected image counts per type
    try:
        img_rows = db.execute(text("SELECT image_type, image_path, thumbnail_path, image_id FROM tank_images WHERE inspection_id = :id"), {"id": inspection_id}).fetchall() or []
        img_count = len(img_rows)
        # Validate image_types master table to calculate expected total counts
        expected_types = db.execute(text("SELECT id, image_type, count FROM image_type")).fetchall() or []
        expected_by_id = {}
        expected_by_name = {}
        expected_total_images = 0
        for et in expected_types:
            if hasattr(et, "_mapping"):
                eid = et._mapping.get("id")
                cnt = et._mapping.get("count") or 1
            elif isinstance(et, dict):
                eid = et.get("id")
                cnt = et.get("count") or 1
            else:
                # tuple fallback
                try:
                    eid, _, cnt = et
                except Exception:
                    continue
            expected_by_id[eid] = int(cnt)
            etname_raw = (et._mapping.get("image_type") if hasattr(et, "_mapping") else (et[1] if isinstance(et, (list,tuple)) and len(et)>1 else str(et).lower()))
            expected_by_name[_norm_name(etname_raw)] = int(cnt)
            expected_total_images += int(cnt)

        if expected_total_images == 0:
            expected_total_images = 15
        if img_count < expected_total_images:
            issues["images"].append({"reason": f"insufficient images: found {img_count}, expected {expected_total_images}"})
        else:
            # Check each image has path & type
            for idx, r in enumerate(img_rows):
                rr = dict(r._mapping) if hasattr(r, "_mapping") else dict(zip(r.keys(), r))
                if not rr.get("image_path"):
                    issues["images"].append({"index": idx, "reason": "image_path missing"})
                if (not rr.get("image_id")) and (not rr.get("image_type")):
                    issues["images"].append({"index": idx, "reason": "image type missing"})

        # Check counts by image id match expected counts (for expected_by_id)
        # Build map of actual counts
        # Create actual counts keyed by id and by normalized name
        actual_counts_by_id = {}
        actual_counts_by_name = {}
        for r in img_rows:
            rr = dict(r._mapping) if hasattr(r, '_mapping') else dict(zip(r.keys(), r))
            rid = rr.get('image_id')
            rname = rr.get('image_type') or None
            if rid is not None:
                try:
                    rid_int = int(rid)
                    actual_counts_by_id[rid_int] = actual_counts_by_id.get(rid_int, 0) + 1
                except Exception:
                    # fallback: maybe it's a string
                    try:
                        rid_int = int(str(rid).strip())
                        actual_counts_by_id[rid_int] = actual_counts_by_id.get(rid_int, 0) + 1
                    except Exception:
                        pass
            if rname:
                rname_norm = _norm_name(rname)
                actual_counts_by_name[rname_norm] = actual_counts_by_name.get(rname_norm, 0) + 1
        # Check expected_by_id
        missing_types = []
        # Check each expected type (by id) using both id and name counts
        for et in expected_types:
            if hasattr(et, '_mapping'):
                eid = et._mapping.get('id')
                etname = (et._mapping.get('image_type') or '').strip().lower()
                expected_ct = int(et._mapping.get('count') or 1)
            elif isinstance(et, dict):
                eid = et.get('id')
                etname = (et.get('image_type') or '').strip().lower()
                expected_ct = int(et.get('count') or 1)
            else:
                try:
                    eid, etname, expected_ct = et
                    etname = str(etname).strip().lower()
                    expected_ct = int(expected_ct or 1)
                except Exception:
                    continue
            # Use name-based matching (more reliable with normalization)
            # Only fall back to ID matching if name is not found
            etname_norm = _norm_name(etname) if etname else None
            actual_ct = 0
            if etname_norm and etname_norm in actual_counts_by_name:
                actual_ct = actual_counts_by_name.get(etname_norm, 0)
            elif isinstance(eid, int) or (isinstance(eid, str) and str(eid).isdigit()):
                try:
                    actual_ct = actual_counts_by_id.get(int(eid), 0)
                except Exception:
                    pass
            # If actual_ct is less than expected, it's missing
            if actual_ct < expected_ct:
                missing_types.append({"image_id": eid, "image_type": etname, "expected": expected_ct, "actual": actual_ct})
        if missing_types:
            issues["images"].append({"reason": "missing image types or counts", "missing": missing_types})

    except Exception as e:
        logger.exception("Error validating images for inspection %s: %s", inspection_id, e)
        return _error(f"Error validating images: {e}", status_code=500)

    # If there are no issues, success
    any_issues = any(len(v) > 0 for v in issues.values())
    if not any_issues:
        # return a simple success and counts
        return _success({"inspection_id": inspection_id, "images_count": img_count, "checklist_count": len(checklist_rows)}, message="All validation checks passed")

    return _success({"inspection_id": inspection_id, "issues": issues}, message="Validation issues found")
