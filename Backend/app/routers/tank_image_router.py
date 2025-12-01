# app/routers/tank_image_router.py
import os
import uuid
from datetime import date, datetime
from typing import Optional, Dict, List, Any, Union
import jwt  # PyJWT
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, status, Header, Request
from pymysql.cursors import DictCursor

from app.database import get_db_connection

try:
    from PIL import Image
except Exception:
    Image = None

# Constants / config
JWT_SECRET = os.getenv("JWT_SECRET", "replace-with-real-secret")
MAX_UPLOAD_SIZE = 5 * 1024 * 1024
THUMBNAIL_SIZE = (200, 200)
UPLOAD_DIR = "uploads"
IMAGES_ROOT_DIR = os.path.join(UPLOAD_DIR, "tank_images_mobile")
if not os.path.exists(IMAGES_ROOT_DIR):
    os.makedirs(IMAGES_ROOT_DIR, exist_ok=True)

# local developer-supplied example images
UPLOADED_FILE_PATH = "/mnt/data/8dbf3773-f49e-4cb0-a01a-35c9bbf8bd09.png"
SWAGGER_IMAGE_PATH = "/mnt/data/95ba5c43-1b45-4c95-b5bf-691349db8047.png"

router = APIRouter(prefix="/api/upload", tags=["upload"])

# ------------------------------------------------------------------
# Default image types (Updated to 15 types; underside split into two)
# ------------------------------------------------------------------
IMAGE_TYPES = {
    "frontview": {"image_type_id": 1, "image_type": "Front View", "description": "General tank photos"},
    "rearview": {"image_type_id": 2, "image_type": "Rear View", "description": "Photos from rear side"},
    "topview": {"image_type_id": 3, "image_type": "Top View", "description": "Photos from top"},
    # undersides now two separate types (undersideview01 = id 4, undersideview02 = id 15)
    "undersideview01": {"image_type_id": 4, "image_type": "Underside View 01", "description": "Underside photo #1"},
    "frontlhview": {"image_type_id": 5, "image_type": "Front LH View", "description": "Left-hand front view"},
    "rearlhview": {"image_type_id": 6, "image_type": "Rear LH View", "description": "Left-hand rear view"},
    "frontrhview": {"image_type_id": 7, "image_type": "Front RH View", "description": "Right-hand front view"},
    "rearrhview": {"image_type_id": 8, "image_type": "Rear RH View", "description": "Right-hand rear view"},
    "lhsideview": {"image_type_id": 9, "image_type": "LH Side View", "description": "Left side view"},
    "rhsideview": {"image_type_id": 10, "image_type": "RH Side View", "description": "Right side view"},
    "valvessectionview": {"image_type_id": 11, "image_type": "Valves Section View", "description": "Valves section photos"},
    "safetyvalve": {"image_type_id": 12, "image_type": "Safety Valve", "description": "Safety valve photos"},
    "levelpressuregauge": {"image_type_id": 13, "image_type": "Level / Pressure Gauge", "description": "Photos showing gauge readings"},
    "vacuumreading": {"image_type_id": 14, "image_type": "Vacuum Reading", "description": "Vacuum reading photos"},
    # new 15th type for second underside image
    "undersideview02": {"image_type_id": 15, "image_type": "Underside View 02", "description": "Underside photo #2"},
}

# ------------------------------------------------------------------
# Auth helper: parse Authorization header, decode JWT, return int id
# ------------------------------------------------------------------
def _get_user_id_from_auth_header(authorization: Optional[str]) -> Optional[int]:
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authorization header")
    token = parts[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256", "HS384", "HS512"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    for key in ("user_id", "emp_id", "id", "sub", "uid"):
        if key in payload:
            try:
                return int(payload[key])
            except Exception:
                try:
                    return int(str(payload[key]))
                except Exception:
                    continue
    return None


def _sanitize_slug(raw: str) -> str:
    """Sanitize a slug by replacing problematic characters and allowing only alphanumerics, underscore and hyphen."""
    if not raw:
        return ""
    s = str(raw).strip().lower()
    s = s.replace("/", "_").replace("\\", "_").replace(" ", "_")
    s = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_"))
    return s

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def save_uploaded_file(file: UploadFile, tank_number: str, image_type_id: Optional[int], index: Optional[int] = None, slug_override: Optional[str] = None) -> dict:
    """
    Save the uploaded file into:
      uploads/tank_images_mobile/{tank_number}/originals/{filename}
    Create thumbnails in:
      uploads/tank_images_mobile/{tank_number}/thumbnails/{thumb_filename}
    Return a dict with image_path (pointing to originals) and thumbnail_path (pointing to thumbnails).
    """
    resolved_slug = None
    try:
        if image_type_id is not None:
            conn = get_db_connection()
            try:
                with conn.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT id, image_type FROM image_type WHERE id = %s LIMIT 1", (image_type_id,))
                    r = cursor.fetchone()
                    if r and r.get("image_type"):
                        # sanitize DB-provided image_type into safe slug
                        slug_raw = str(r["image_type"]).strip().lower()
                        # replace slashes and spaces with underscore
                        slug_raw = slug_raw.replace("/", "_").replace("\\", "_").replace(" ", "_")
                        slug = "".join(ch for ch in slug_raw if ch.isalnum() or ch in ("-", "_"))
                        resolved_slug = slug or f"img{image_type_id}"
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
    except Exception:
        resolved_slug = None

    if not resolved_slug:
        resolved_slug = f"image_{image_type_id}" if image_type_id is not None else "image"

    if slug_override:
        resolved_slug = slug_override

    file_extension = os.path.splitext(file.filename)[1] if file.filename else ".jpg"
    # if index provided, append an ordinal suffix to the slug (undersideview01, undersideview02)
    if index is not None:
        resolved_slug_with_index = f"{resolved_slug}{index:02d}"
        unique_filename = f"{tank_number}_{resolved_slug_with_index}_{uuid.uuid4().hex}{file_extension}"
    else:
        unique_filename = f"{tank_number}_{resolved_slug}_{uuid.uuid4().hex}{file_extension}"

    # Create directories: tank folder, originals and thumbnails
    tank_dir_fs = os.path.join(IMAGES_ROOT_DIR, tank_number)
    originals_dir = os.path.join(tank_dir_fs, "originals")
    thumbnails_dir = os.path.join(tank_dir_fs, "thumbnails")
    os.makedirs(originals_dir, exist_ok=True)
    os.makedirs(thumbnails_dir, exist_ok=True)

    file_path_fs = os.path.join(originals_dir, unique_filename)
    total = 0
    chunk_size = 64 * 1024
    try:
        with open(file_path_fs, "wb") as buffer:
            while True:
                chunk = file.file.read(chunk_size)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_UPLOAD_SIZE:
                    buffer.close()
                    try:
                        os.remove(file_path_fs)
                    except Exception:
                        pass
                    raise HTTPException(status_code=413, detail=f"File too large. Limit is {MAX_UPLOAD_SIZE} bytes")
                buffer.write(chunk)
    finally:
        try:
            file.file.seek(0)
        except Exception:
            pass

    result = {
        # image_path points to originals subfolder
        "image_path": f"{tank_number}/originals/{unique_filename}",
        "thumbnail_path": None,
        "size": total,
        "resolved_image_type_slug": resolved_slug if index is None else resolved_slug_with_index,
        "image_type_id": image_type_id
    }

    # Generate thumbnail into thumbnails_dir
    if Image is not None:
        try:
            thumb_slug = result.get("resolved_image_type_slug")
            thumb_name = f"{tank_number}_{thumb_slug}_{uuid.uuid4().hex}_thumb.jpg"
            thumb_path_fs = os.path.join(thumbnails_dir, thumb_name)
            with Image.open(file_path_fs) as img:
                img.thumbnail(THUMBNAIL_SIZE)
                img.convert("RGB").save(thumb_path_fs, format="JPEG")
            result["thumbnail_path"] = f"{tank_number}/thumbnails/{thumb_name}"
        except Exception as e:
            print(f"Warning: thumbnail generation failed for {file_path_fs}: {e}")

    return result

def delete_file(file_path: str):
    """
    Delete a saved file. Also attempt to delete matching thumbnails.
    Accepts file_path like 'TANKNUM/originals/filename.jpg' or 'TANKNUM/thumbnails/thumb.jpg'.
    """
    try:
        full_path = os.path.join(IMAGES_ROOT_DIR, *file_path.split("/"))
        if os.path.exists(full_path):
            os.remove(full_path)
        # Attempt to remove linked thumbnails/originals
        try:
            # If we deleted an original, try to delete any matching thumbs in thumbnails folder.
            # If we deleted a thumbnail, that's already removed.
            parts = file_path.split("/")
            if len(parts) >= 3:
                tank_number = parts[0]
                # base name without extension (original filename's base)
                base = os.path.splitext(parts[-1])[0]
                # path to thumbnails folder
                thumbs_folder = os.path.join(IMAGES_ROOT_DIR, tank_number, "thumbnails")
                if os.path.isdir(thumbs_folder):
                    for fn in os.listdir(thumbs_folder):
                        # remove any thumb file that contains the base slug
                        if base in fn:
                            try:
                                os.remove(os.path.join(thumbs_folder, fn))
                            except Exception:
                                pass
                # also attempt to remove originals that match if a thumbnail was removed
                originals_folder = os.path.join(IMAGES_ROOT_DIR, tank_number, "originals")
                if os.path.isdir(originals_folder):
                    for fn in os.listdir(originals_folder):
                        if base in fn:
                            try:
                                os.remove(os.path.join(originals_folder, fn))
                            except Exception:
                                pass
        except Exception:
            pass
    except Exception as e:
        print(f"Error deleting file {file_path}: {e}")

def validate_tank(tank_number: str):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM tank_header WHERE tank_number = %s", (tank_number,))
            if cursor.fetchone()["count"] == 0:
                # Fallback: some environments populate `tank_details` instead of `tank_header`.
                # Check `tank_details` for the tank_number before failing.
                try:
                    cursor.execute("SELECT COUNT(*) as cnt FROM tank_details WHERE tank_number = %s", (tank_number,))
                    r = cursor.fetchone()
                    if not r or r.get("cnt", 0) == 0:
                        raise HTTPException(status_code=404, detail="Tank not found")
                except HTTPException:
                    raise
                except Exception:
                    # If the fallback check fails unexpectedly, treat as not found
                    raise HTTPException(status_code=404, detail="Tank not found")
    finally:
        connection.close()
    return True

def _derive_latest_inspection_id(cursor, tank_number: str) -> Optional[int]:
    try:
        cursor.execute(
            "SELECT inspection_id FROM tank_inspection_details WHERE tank_number=%s ORDER BY inspection_date DESC, inspection_id DESC LIMIT 1",
            (tank_number,),
        )
        row = cursor.fetchone()
        return row.get("inspection_id") if row and row.get("inspection_id") is not None else None
    except Exception:
        return None

# ------------------------------------------------------------------
# ENDPOINT: GET image types
# ------------------------------------------------------------------
@router.get("/types")
def get_image_types():
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cur:
            # Added count column to query
            cur.execute("SELECT id, image_type, description, count FROM image_type ORDER BY id ASC")
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        "success": True,
        "data": [
            {
                "image_type_id": r["id"],
                "image_type": r["image_type"],
                "description": r["description"],
                "count": r.get("count", 1)  # Return DB limit, default 1
            }
            for r in rows
        ]
    }

# ------------------------------------------------------------------
# ENDPOINT: Batch Upload (Moved to TOP to avoid route conflict)
# POST /api/upload/batch/{tank_number}
# ------------------------------------------------------------------
# Make sure to import Union at the top
@router.post("/batch/{inspection_id}", status_code=status.HTTP_201_CREATED)
async def batch_upload_images(
    request: Request,
    inspection_id: int,
    
    # Single file uploads
    frontview: Optional[Union[UploadFile, str]] = File(None),
    rearview: Optional[Union[UploadFile, str]] = File(None),
    topview: Optional[Union[UploadFile, str]] = File(None),
    
    # Two separate underside files now (field names requested):
    undersideview01: Optional[Union[UploadFile, str]] = File(None),
    undersideview02: Optional[Union[UploadFile, str]] = File(None),
    
    frontlhview: Optional[Union[UploadFile, str]] = File(None),
    rearlhview: Optional[Union[UploadFile, str]] = File(None),
    frontrhview: Optional[Union[UploadFile, str]] = File(None),
    rearrhview: Optional[Union[UploadFile, str]] = File(None),
    lhsideview: Optional[Union[UploadFile, str]] = File(None),
    rhsideview: Optional[Union[UploadFile, str]] = File(None),
    valvessectionview: Optional[Union[UploadFile, str]] = File(None),
    safetyvalve: Optional[Union[UploadFile, str]] = File(None),
    levelpressuregauge: Optional[Union[UploadFile, str]] = File(None),
    vacuumreading: Optional[Union[UploadFile, str]] = File(None),
    Authorization: Optional[str] = Header(None),
):
    """
    Batch upload with DYNAMIC LIMITS.
    Accepts file uploads for all image types. Underside images are now uploaded as two separate fields:
    undersideview01 and undersideview02 (single file each).
    """

    # --- SANITIZATION: Clean up None/empty values ---
    def clean_input(val):
        """Remove None or empty values."""
        if val is None:
            return None
        # If a string was passed instead of an UploadFile (swagger quirk), drop it
        if isinstance(val, str):
            return None
        return val

    # Clean single files
    frontview = clean_input(frontview)
    rearview = clean_input(rearview)
    topview = clean_input(topview)
    undersideview01 = clean_input(undersideview01)
    undersideview02 = clean_input(undersideview02)
    frontlhview = clean_input(frontlhview)
    rearlhview = clean_input(rearlhview)
    frontrhview = clean_input(frontrhview)
    rearrhview = clean_input(rearrhview)
    lhsideview = clean_input(lhsideview)
    rhsideview = clean_input(rhsideview)
    valvessectionview = clean_input(valvessectionview)
    safetyvalve = clean_input(safetyvalve)
    levelpressuregauge = clean_input(levelpressuregauge)
    vacuumreading = clean_input(vacuumreading)

    # If either underside field is missing, attempt to pick up from raw form (robustness for Swagger quirks)
    # This tries to extract file parts named 'undersideview01' or 'undersideview02' from the raw form if binding failed.
    if undersideview01 is None or undersideview02 is None:
        try:
            form = await request.form()
            # form.get returns last value; use get if available
            if undersideview01 is None:
                v = form.get("undersideview01") if hasattr(form, "get") else None
                if isinstance(v, UploadFile):
                    undersideview01 = v
            if undersideview02 is None:
                v2 = form.get("undersideview02") if hasattr(form, "get") else None
                if isinstance(v2, UploadFile):
                    undersideview02 = v2
        except Exception:
            pass

    # Validate underside files if present: both optional individually, but when saved they will become undersideview01/02
    if undersideview01:
        if not getattr(undersideview01, "content_type", "").startswith("image/"):
            raise HTTPException(status_code=400, detail="undersideview01 must be an image (image/*).")
    if undersideview02:
        if not getattr(undersideview02, "content_type", "").startswith("image/"):
            raise HTTPException(status_code=400, detail="undersideview02 must be an image (image/*).")

    # 1. Fetch Dynamic Counts
    count_limits = {}
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT id, count FROM image_type")
            rows = cursor.fetchall()
            for r in rows:
                count_limits[r['id']] = r.get('count', 1)
    finally:
        conn.close()

    # 2. Map Inputs
    uploaded_files_map = {
        "frontview": frontview,
        "rearview": rearview,
        "topview": topview,
        "undersideview01": undersideview01,
        "undersideview02": undersideview02,
        "frontlhview": frontlhview,
        "rearlhview": rearlhview,
        "frontrhview": frontrhview,
        "rearrhview": rearrhview,
        "lhsideview": lhsideview,
        "rhsideview": rhsideview,
        "valvessectionview": valvessectionview,
        "safetyvalve": safetyvalve,
        "levelpressuregauge": levelpressuregauge,
        "vacuumreading": vacuumreading,
    }

    # 3. Process Logic
    files_to_process = {}
    for key, value in uploaded_files_map.items():
        if value:
            # value is a single UploadFile (or File-like) — convert to list for uniform handling
            file_list = [value]

            type_info = IMAGE_TYPES.get(key)
            if not type_info:
                # unknown image type slug — skip silently
                continue
            type_id = type_info["image_type_id"]
            db_limit = count_limits.get(type_id, 1)

            # For the new undersideview01/02 fields we want to save each as its respective index under base slug 'undersideview'
            # But enforce DB limit if it's less than 1 (rare)
            final_list = file_list[:db_limit]

            if len(final_list) > 0:
                files_to_process[key] = final_list

    if not files_to_process:
        raise HTTPException(status_code=400, detail="No valid files provided")

    # Resolve tank_number from inspection_id and validate
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT tank_number FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1", (inspection_id,))
            rr = cursor.fetchone()
            if not rr or not rr.get("tank_number"):
                raise HTTPException(status_code=400, detail="Invalid inspection_id or missing tank_number")
            tank_number = rr.get("tank_number")
    finally:
        conn.close()

    #validate_tank(tank_number)

    # 4. Auth
    token_sub = _get_user_id_from_auth_header(Authorization)
    authenticated_emp_id = None
    if token_sub:
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT emp_id FROM users WHERE id = %s LIMIT 1", (token_sub,))
                urow = cursor.fetchone()
                if urow and urow.get("emp_id"):
                    authenticated_emp_id = int(urow.get("emp_id"))
        finally:
            conn.close()

    saved_file_paths = []
    successful_inserts = []
    
    # 5. Database Transaction
    conn = get_db_connection()
    try:
        conn.begin()
        with conn.cursor(DictCursor) as cursor:
            if authenticated_emp_id:
                emp_to_use = authenticated_emp_id
            else:
                # fallback to the operator_id for this inspection
                cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1", (inspection_id,))
                op = cursor.fetchone()
                emp_to_use = op.get("operator_id") if op else None

            derived_insp_id = inspection_id

            for slug_key, file_list in files_to_process.items():
                type_info = IMAGE_TYPES.get(slug_key)
                image_type_id = type_info["image_type_id"]

                for idx, file_obj in enumerate(file_list, start=1):
                    # Final safety check
                    if not hasattr(file_obj, 'content_type') or not file_obj.content_type.startswith('image/'):
                        # skip non-image parts
                        continue

                    # For underside fields we want to save both under the same base slug 'undersideview'
                    slug_override = None
                    seq_index = None
                    if slug_key.startswith("undersideview"):
                        # determine 01 vs 02 suffix from key
                        if slug_key.endswith("01"):
                            slug_override = "undersideview"
                            seq_index = 1
                        elif slug_key.endswith("02"):
                            slug_override = "undersideview"
                            seq_index = 2
                        else:
                            # fallback: if key is undersideviewXX, try to parse digits
                            try:
                                parts = slug_key.replace("undersideview", "")
                                num = int(parts)
                                slug_override = "undersideview"
                                seq_index = num
                            except Exception:
                                slug_override = None
                                seq_index = None

                    # ... (inside your loop) ...

                    saved_info = save_uploaded_file(file_obj, tank_number, image_type_id, index=seq_index, slug_override=slug_override)
                    saved_file_paths.append(saved_info["image_path"]) 
                    final_slug = saved_info.get("resolved_image_type_slug") or slug_key

                    # --- REPLACE WITH THIS FIXED QUERY ---
                    cursor.execute("""
                        INSERT INTO tank_images 
                        (emp_id, inspection_id, image_id, tank_number, image_type, image_path, thumbnail_path, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        emp_to_use, 
                        derived_insp_id, 
                        image_type_id, 
                        tank_number, 
                        final_slug, 
                        saved_info["image_path"],
                        saved_info.get("thumbnail_path")  # <--- This is the important addition!
                    ))
                    # -------------------------------------

                    successful_inserts.append({
                        "image_type_id": image_type_id,
                        "filename": saved_info["image_path"]
                    })

        conn.commit()
        return {"success": True, "message": f"Uploaded {len(successful_inserts)} images.", "data": successful_inserts}

    except Exception as e:
        conn.rollback()
        for p in saved_file_paths: delete_file(p)
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=f"Batch upload failed: {str(e)}")
    finally:
        conn.close()


# ------------------------------------------------------------------
# ENDPOINT: Get images by inspection (GET) - modified output format
# ------------------------------------------------------------------
@router.get("/images/inspection/{inspection_id}")
def get_images_by_inspection(inspection_id: int):
    """
    Returns:
    {
      "success": True,
      "data": {
         "inspection_id": "<inspection_id>",
         "tank_id": "<tank_id>",
         "emp_id": "<emp_id>",
         "images": [ ... image objects ... ]
      }
    }
    Each image object contains image_type_id (from image_id) and tank_id (same tank_id as above).
    """
    try:
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT * FROM tank_images WHERE inspection_id = %s ORDER BY created_at ASC", (inspection_id,))
                rows = cursor.fetchall() or []

                # derive inspection-level tank_id and emp_id from tank_inspection_details
                cursor.execute("SELECT tank_id, tank_number, emp_id, operator_id FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1", (inspection_id,))
                insp_row = cursor.fetchone() or {}
        finally:
            conn.close()

        # derive tank_id and emp_id for top-level output
        tank_id = insp_row.get("tank_id") if insp_row.get("tank_id") is not None else ""
        # prefer emp_id from inspection row if set else operator_id else first image emp_id
        emp_id = insp_row.get("emp_id") if insp_row.get("emp_id") is not None else insp_row.get("operator_id")

        # build images list
        enriched_rows = []
        for row in rows:
            thumb = None
            try:
                base = row.get("image_path")  # now value like "TANKNUM/originals/filename.jpg"
                if base:
                    # find thumbnails under tank_number/thumbnails
                    parts = base.split("/")
                    if len(parts) >= 3:
                        tank_number = parts[0]
                        name = os.path.splitext(os.path.basename(base))[0]  # e.g. "TANKNUM_slug_uuid"
                        thumbnails_abs = os.path.join(IMAGES_ROOT_DIR, tank_number, "thumbnails")
                        if os.path.isdir(thumbnails_abs):
                            prefix = "_".join(name.split('_')[:2]) + "_"
                            for fn in os.listdir(thumbnails_abs):
                                if fn.startswith(prefix) and fn.endswith('_thumb.jpg'):
                                    thumb = f"{tank_number}/thumbnails/{fn}"
                                    break
            except Exception:
                thumb = None

            # Build image object: replace image_type and tank_number with image_type_id and tank_id
            image_obj = dict(row)
            # remove/replace keys as requested
            image_obj["image_type_id"] = image_obj.get("image_id")
            # use tank_id derived from inspection row (consistent for this inspection)
            image_obj["tank_id"] = tank_id if tank_id != "" else image_obj.get("tank_number", "")
            # remove old fields if present
            if "image_type" in image_obj:
                image_obj.pop("image_type", None)
            if "tank_number" in image_obj:
                image_obj.pop("tank_number", None)

            image_obj["thumbnail_path"] = thumb
            enriched_rows.append(image_obj)

        # Prepare response data object with inspection_id, tank_id, emp_id outside images list
        resp_data = {
            "inspection_id": str(inspection_id),
            "tank_id": str(tank_id) if tank_id != "" else "",
            "emp_id": str(emp_id) if emp_id is not None else "",
            "images": enriched_rows
        }

        return {"success": True, "data": resp_data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------
# ENDPOINT: Replace image by id (PUT) - requires Authorization header
# ------------------------------------------------------------------

@router.put("/images/inspection/{inspection_id}")
async def replace_images_by_inspection_id(
    request: Request,
    inspection_id: int,
    frontview: Optional[Union[UploadFile, str]] = File(None),
    rearview: Optional[Union[UploadFile, str]] = File(None),
    topview: Optional[Union[UploadFile, str]] = File(None),
    undersideview01: Optional[Union[UploadFile, str]] = File(None),
    undersideview02: Optional[Union[UploadFile, str]] = File(None),
    frontlhview: Optional[Union[UploadFile, str]] = File(None),
    rearlhview: Optional[Union[UploadFile, str]] = File(None),
    frontrhview: Optional[Union[UploadFile, str]] = File(None),
    rearrhview: Optional[Union[UploadFile, str]] = File(None),
    lhsideview: Optional[Union[UploadFile, str]] = File(None),
    rhsideview: Optional[Union[UploadFile, str]] = File(None),
    valvessectionview: Optional[Union[UploadFile, str]] = File(None),
    safetyvalve: Optional[Union[UploadFile, str]] = File(None),
    levelpressuregauge: Optional[Union[UploadFile, str]] = File(None),
    vacuumreading: Optional[Union[UploadFile, str]] = File(None),
    Authorization: Optional[str] = Header(None),
):
    """
    Replace images for the provided image types for a given inspection_id (partial update).
    This will only delete existing images for the image types included in the request and insert the newly provided files.
    """
    # --- SANITIZATION: Clean up None/empty values ---
    def clean_input(val):
        if val is None: return None
        if isinstance(val, str): return None
        return val

    frontview = clean_input(frontview)
    rearview = clean_input(rearview)
    topview = clean_input(topview)
    undersideview01 = clean_input(undersideview01)
    undersideview02 = clean_input(undersideview02)
    frontlhview = clean_input(frontlhview)
    rearlhview = clean_input(rearlhview)
    frontrhview = clean_input(frontrhview)
    rearrhview = clean_input(rearrhview)
    lhsideview = clean_input(lhsideview)
    rhsideview = clean_input(rhsideview)
    valvessectionview = clean_input(valvessectionview)
    safetyvalve = clean_input(safetyvalve)
    levelpressuregauge = clean_input(levelpressuregauge)
    vacuumreading = clean_input(vacuumreading)

    # Swagger quirk handling for underside views
    if undersideview01 is None or undersideview02 is None:
        try:
            form = await request.form()
            if undersideview01 is None:
                v = form.get("undersideview01") if hasattr(form, "get") else None
                if isinstance(v, UploadFile): undersideview01 = v
            if undersideview02 is None:
                v2 = form.get("undersideview02") if hasattr(form, "get") else None
                if isinstance(v2, UploadFile): undersideview02 = v2
        except Exception:
            pass

    # Validate content types
    for f in [undersideview01, undersideview02]:
        if f and not getattr(f, "content_type", "").startswith("image/"):
            raise HTTPException(status_code=400, detail="Underside views must be images.")

    # 1. Fetch Dynamic Counts
    count_limits = {}
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT id, count FROM image_type")
            for r in cursor.fetchall():
                count_limits[r['id']] = r.get('count', 1)
    finally:
        conn.close()

    # 2. Map Inputs
    uploaded_files_map = {
        "frontview": frontview,
        "rearview": rearview,
        "topview": topview,
        "undersideview01": undersideview01,
        "undersideview02": undersideview02,
        "frontlhview": frontlhview,
        "rearlhview": rearlhview,
        "frontrhview": frontrhview,
        "rearrhview": rearrhview,
        "lhsideview": lhsideview,
        "rhsideview": rhsideview,
        "valvessectionview": valvessectionview,
        "safetyvalve": safetyvalve,
        "levelpressuregauge": levelpressuregauge,
        "vacuumreading": vacuumreading,
    }

    # 3. Process Logic
    files_to_process = {}
    for key, value in uploaded_files_map.items():
        if value:
            file_list = [value]
            type_info = IMAGE_TYPES.get(key)
            if not type_info: continue
            
            type_id = type_info["image_type_id"]
            db_limit = count_limits.get(type_id, 1)
            final_list = file_list[:db_limit]
            
            if len(final_list) > 0:
                files_to_process[key] = final_list

    if not files_to_process:
        raise HTTPException(status_code=400, detail="No valid files provided to replace images.")

    # Resolve tank_number
    conn = get_db_connection()
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT tank_number FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1", (inspection_id,))
            rr = cursor.fetchone()
            if not rr or not rr.get("tank_number"):
                raise HTTPException(status_code=404, detail="Inspection not found or missing tank_number")
            tank_number = rr.get("tank_number")
    finally:
        conn.close()

    # 4. Auth
    token_sub = _get_user_id_from_auth_header(Authorization)
    authenticated_emp_id = None
    if token_sub:
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT emp_id FROM users WHERE id = %s LIMIT 1", (token_sub,))
                urow = cursor.fetchone()
                if urow and urow.get("emp_id"):
                    authenticated_emp_id = int(urow.get("emp_id"))
        finally:
            conn.close()

    saved_file_paths = []
    successful_inserts = []

    # 5. Database Transaction (Delete Old -> Insert New)
    conn = get_db_connection()
    try:
        conn.begin()
        with conn.cursor(DictCursor) as cursor:
            # Determine emp_id
            if authenticated_emp_id:
                emp_to_use = authenticated_emp_id
            else:
                cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1", (inspection_id,))
                op = cursor.fetchone()
                emp_to_use = op.get("operator_id") if op else None

            # --- DELETE OLD IMAGES (only for provided image types) ---
            # Build list of image_type IDs we're about to replace (partial update)
            image_type_ids_to_replace = [IMAGE_TYPES[k]["image_type_id"] for k in files_to_process.keys() if k in IMAGE_TYPES]
            old_rows = []
            if image_type_ids_to_replace:
                placeholders = ",".join(["%s"] * len(image_type_ids_to_replace))
                # fetch old rows for only these image types
                cursor.execute(f"SELECT id, image_id, image_path, thumbnail_path FROM tank_images WHERE inspection_id = %s AND image_id IN ({placeholders})", (inspection_id, *image_type_ids_to_replace))
                old_rows = cursor.fetchall() or []
                # delete only those image rows
                cursor.execute(f"DELETE FROM tank_images WHERE inspection_id = %s AND image_id IN ({placeholders})", (inspection_id, *image_type_ids_to_replace))
            
            # Cleanup old files from disk (only for replaced types)
            for row in old_rows:
                if row.get("image_path"): delete_file(row["image_path"])
                if row.get("thumbnail_path"): delete_file(row["thumbnail_path"])

            # --- INSERT NEW IMAGES ---
            for slug_key, file_list in files_to_process.items():
                type_info = IMAGE_TYPES.get(slug_key)
                image_type_id = type_info["image_type_id"]

                for idx, file_obj in enumerate(file_list, start=1):
                    if not hasattr(file_obj, 'content_type') or not file_obj.content_type.startswith('image/'):
                        continue

                    # Underside logic
                    slug_override = None
                    seq_index = None
                    if slug_key.startswith("undersideview"):
                        if slug_key.endswith("01"):
                            slug_override = "undersideview"
                            seq_index = 1
                        elif slug_key.endswith("02"):
                            slug_override = "undersideview"
                            seq_index = 2
                        else:
                            try:
                                parts = slug_key.replace("undersideview", "")
                                num = int(parts)
                                slug_override = "undersideview"
                                seq_index = num
                            except: pass

                    saved_info = save_uploaded_file(file_obj, tank_number, image_type_id, index=seq_index, slug_override=slug_override)
                    saved_file_paths.append(saved_info["image_path"]) 
                    final_slug = saved_info.get("resolved_image_type_slug") or slug_key

                    cursor.execute("""
                        INSERT INTO tank_images 
                        (emp_id, inspection_id, image_id, tank_number, image_type, image_path, thumbnail_path, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        emp_to_use, 
                        inspection_id, 
                        image_type_id, 
                        tank_number, 
                        final_slug, 
                        saved_info["image_path"],
                        saved_info.get("thumbnail_path")
                    ))

                    successful_inserts.append({
                        "image_type_id": image_type_id,
                        "filename": saved_info["image_path"]
                    })

        conn.commit()
        return {"success": True, "message": f"Replaced images. Uploaded {len(successful_inserts)} new images.", "data": successful_inserts}

    except Exception as e:
        conn.rollback()
        for p in saved_file_paths: delete_file(p)
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=f"Replace images failed: {str(e)}")
    finally:
        conn.close()

# ------------------------------------------------------------------
# ENDPOINT: Delete single image by id (DELETE) - requires Authorization header
# ------------------------------------------------------------------
@router.delete("/images/{id}")
def delete_image_by_id_new(id: int, Authorization: Optional[str] = Header(None)):
    try:
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT image_path, tank_number FROM tank_images WHERE id=%s", (id,))
                row = cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Image id not found")

                authenticated_emp_id = _get_user_id_from_auth_header(Authorization)

                legacy_emp = None
                if row.get("tank_number"):
                    cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE tank_number=%s ORDER BY inspection_date DESC, inspection_id DESC LIMIT 1", (row.get("tank_number"),))
                    op = cursor.fetchone()
                    legacy_emp = op.get("operator_id") if op and op.get("operator_id") is not None else None

                emp_to_use = authenticated_emp_id if authenticated_emp_id is not None else legacy_emp

                try:
                    cursor.execute("UPDATE tank_images SET emp_id=%s, updated_at=NOW() WHERE id=%s", (emp_to_use, id))
                    conn.commit()
                except Exception:
                    pass

                cursor.execute("DELETE FROM tank_images WHERE id=%s", (id,))
                conn.commit()
        finally:
            conn.close()

        if row.get("image_path"):
            delete_file(row["image_path"])

        return {"success": True, "message": f"Image id {id} deleted", "deleted": 1}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------
# ENDPOINT: Batch delete by comma separated ids (DELETE) - requires Authorization header
# ------------------------------------------------------------------
@router.delete("/images/batch/{ids}")
def delete_images_by_ids(ids: str, Authorization: Optional[str] = Header(None)):
    try:
        if not ids:
            raise HTTPException(status_code=400, detail="No ids provided")

        id_list = []
        for part in ids.split(","):
            try:
                iid = int(part.strip())
                id_list.append(iid)
            except Exception:
                continue

        if not id_list:
            raise HTTPException(status_code=400, detail="No valid image ids provided")

        placeholders = ",".join(["%s"] * len(id_list))
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(f"SELECT image_path, id, tank_number FROM tank_images WHERE id IN ({placeholders})", tuple(id_list))
                rows = cursor.fetchall() or []

                authenticated_emp_id = _get_user_id_from_auth_header(Authorization)
                legacy_emp = None
                if rows:
                    first_tank = rows[0].get("tank_number")
                    if first_tank:
                        cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE tank_number=%s ORDER BY inspection_date DESC, inspection_id DESC LIMIT 1", (first_tank,))
                        op = cursor.fetchone()
                        legacy_emp = op.get("operator_id") if op and op.get("operator_id") is not None else None

                emp_to_use = authenticated_emp_id if authenticated_emp_id is not None else legacy_emp

                try:
                    cursor.execute(f"UPDATE tank_images SET emp_id=%s, updated_at=NOW() WHERE id IN ({placeholders})", (emp_to_use, *id_list))
                    cursor.execute(f"DELETE FROM tank_images WHERE id IN ({placeholders})", tuple(id_list))
                    conn.commit()
                except Exception:
                    raise

        finally:
            conn.close()

        deleted_paths = []
        for r in rows:
            p = r.get("image_path")
            if p:
                delete_file(p)
                deleted_paths.append(p)

        return {"success": True, "message": f"Deleted {len(rows)} images", "deleted_count": len(rows), "deleted_paths": deleted_paths}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------
# ENDPOINT: Legacy delete-by-tank (kept) - requires Authorization header
# ------------------------------------------------------------------

@router.delete("/images/inspection/{inspection_id}")
def delete_images_by_inspection(
    inspection_id: int,
    Authorization: Optional[str] = Header(None),
):
    try:
        token_sub = _get_user_id_from_auth_header(Authorization)
        if token_sub is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization required")

        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT emp_id FROM users WHERE id = %s LIMIT 1", (token_sub,))
                urow = cursor.fetchone()
                if not urow or urow.get("emp_id") is None:
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authenticated user not found")
                try:
                    authenticated_emp_id = int(urow.get("emp_id"))
                except Exception:
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid emp_id for user")
        finally:
            conn.close()

        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(
                    "SELECT id, image_path FROM tank_images WHERE inspection_id = %s",
                    (inspection_id,),
                )
                rows = cursor.fetchall() or []

                if not rows:
                    return {
                        "success": True,
                        "message": f"No images found for inspection_id {inspection_id}",
                        "deleted_count": 0,
                        "deleted_paths": [],
                        "uploaded_file_local_path": UPLOADED_FILE_PATH
                    }

                ids = [r["id"] for r in rows]
                placeholders = ",".join(["%s"] * len(ids))

                try:
                    cursor.execute(
                        f"UPDATE tank_images SET emp_id=%s, updated_at=NOW() WHERE id IN ({placeholders})",
                        (authenticated_emp_id, *ids)
                    )
                except Exception:
                    pass

                cursor.execute(f"DELETE FROM tank_images WHERE id IN ({placeholders})", tuple(ids))
                conn.commit()

                deleted_paths = [r.get("image_path") for r in rows if r.get("image_path")]
        finally:
            conn.close()

        for p in deleted_paths:
            try:
                delete_file(p)
            except Exception:
                pass

        return {
            "success": True,
            "message": f"Deleted {len(deleted_paths)} images for inspection_id {inspection_id}",
            "deleted_count": len(deleted_paths),
            "deleted_paths": deleted_paths,
            "uploaded_file_local_path": UPLOADED_FILE_PATH
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
