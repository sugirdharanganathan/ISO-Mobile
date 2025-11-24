# app/routers/tank_image_router.py
import os
import uuid
from datetime import date, datetime
from typing import Optional, Dict, List, Any, Union
import jwt  # PyJWT
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, status, Header
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
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

# local developer-supplied example images
UPLOADED_FILE_PATH = "/mnt/data/8dbf3773-f49e-4cb0-a01a-35c9bbf8bd09.png"
SWAGGER_IMAGE_PATH = "/mnt/data/95ba5c43-1b45-4c95-b5bf-691349db8047.png"

router = APIRouter(prefix="/api/upload", tags=["upload"])

# ------------------------------------------------------------------
# Default image types (Updated to match DB: Underside ID=4)
# ------------------------------------------------------------------
IMAGE_TYPES = {
    "frontview": {"image_type_id": 1, "image_type": "Front View", "description": "General tank photos"},
    "rearview": {"image_type_id": 2, "image_type": "Rear View", "description": "Photos from rear side"},
    "topview": {"image_type_id": 3, "image_type": "Top View", "description": "Photos from top"},
    "undersideview": {"image_type_id": 4, "image_type": "Underside View", "description": "Photos of underside"},
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

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def save_uploaded_file(file: UploadFile, tank_number: str, image_type_id: Optional[int]) -> dict:
    resolved_slug = None
    try:
        if image_type_id is not None:
            conn = get_db_connection()
            try:
                with conn.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT id, image_type FROM image_type WHERE id = %s LIMIT 1", (image_type_id,))
                    r = cursor.fetchone()
                    if r and r.get("image_type"):
                        slug = str(r["image_type"]).strip().lower().replace(" ", "")
                        slug = "".join(ch for ch in slug if ch.isalnum() or ch in ("-", "_"))
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

    file_extension = os.path.splitext(file.filename)[1] if file.filename else ".jpg"
    unique_filename = f"{tank_number}_{resolved_slug}_{uuid.uuid4().hex}{file_extension}"

    tank_dir_fs = os.path.join(UPLOAD_DIR, tank_number)
    os.makedirs(tank_dir_fs, exist_ok=True)

    file_path_fs = os.path.join(tank_dir_fs, unique_filename)
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
        "image_path": f"{tank_number}/{unique_filename}",
        "thumbnail_path": None,
        "size": total,
        "resolved_image_type_slug": resolved_slug,
        "image_type_id": image_type_id
    }

    if Image is not None:
        try:
            thumb_name = f"{tank_number}_{resolved_slug}_{uuid.uuid4().hex}_thumb.jpg"
            thumb_path_fs = os.path.join(tank_dir_fs, thumb_name)
            with Image.open(file_path_fs) as img:
                img.thumbnail(THUMBNAIL_SIZE)
                img.convert("RGB").save(thumb_path_fs, format="JPEG")
            result["thumbnail_path"] = f"{tank_number}/{thumb_name}"
        except Exception as e:
            print(f"Warning: thumbnail generation failed for {file_path_fs}: {e}")

    return result

def delete_file(file_path: str):
    try:
        full_path = os.path.join(UPLOAD_DIR, *file_path.split("/"))
        if os.path.exists(full_path):
            os.remove(full_path)
        try:
            folder = os.path.dirname(full_path)
            base = os.path.splitext(os.path.basename(full_path))[0]
            if os.path.isdir(folder):
                for fn in os.listdir(folder):
                    if 'thumb' in fn and base in fn:
                        try:
                            os.remove(os.path.join(folder, fn))
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
from typing import Optional, List, Union

@router.post("/batch/{inspection_id}", status_code=status.HTTP_201_CREATED)
async def batch_upload_images(
    inspection_id: int,
    
    # Single file uploads
    frontview: Optional[UploadFile] = File(None),
    rearview: Optional[UploadFile] = File(None),
    topview: Optional[UploadFile] = File(None),
    
    # Multiple file uploads for underside view
    undersideview: Optional[List[UploadFile]] = File(None), 
    
    frontlhview: Optional[UploadFile] = File(None),
    rearlhview: Optional[UploadFile] = File(None),
    frontrhview: Optional[UploadFile] = File(None),
    rearrhview: Optional[UploadFile] = File(None),
    lhsideview: Optional[UploadFile] = File(None),
    rhsideview: Optional[UploadFile] = File(None),
    valvessectionview: Optional[UploadFile] = File(None),
    safetyvalve: Optional[UploadFile] = File(None),
    levelpressuregauge: Optional[UploadFile] = File(None),
    vacuumreading: Optional[UploadFile] = File(None),
    Authorization: Optional[str] = Header(None),
):
    """
    Batch upload with DYNAMIC LIMITS.
    Accepts file uploads for all image types. For undersideview, you can upload multiple files.
    """

    # --- SANITIZATION: Clean up None/empty values ---
    def clean_input(val):
        """Remove None or empty values."""
        if val is None:
            return None
        if isinstance(val, str):  # Fallback: if somehow a string is sent, ignore it
            return None
        return val

    # Clean single files
    frontview = clean_input(frontview)
    rearview = clean_input(rearview)
    topview = clean_input(topview)
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

    # Clean List (Underside View) - keep only valid UploadFile objects
    if undersideview:
        undersideview = [f for f in undersideview if isinstance(f, UploadFile) and f is not None]
        if not undersideview:
            undersideview = None
    # ----------------------------------------------------------

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
        "undersideview": undersideview,
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
            # We already cleaned strings, so value is guaranteed to be File or List[File]
            file_list = value if isinstance(value, list) else [value]
            
            type_info = IMAGE_TYPES.get(key)
            if not type_info: continue
            type_id = type_info["image_type_id"]
            db_limit = count_limits.get(type_id, 1)

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

    validate_tank(tank_number)

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

                for file_obj in file_list:
                    # Final safety check
                    if not hasattr(file_obj, 'content_type') or not file_obj.content_type.startswith('image/'):
                        continue

                    saved_info = save_uploaded_file(file_obj, tank_number, image_type_id)
                    saved_file_paths.append(saved_info["image_path"]) 
                    final_slug = saved_info.get("resolved_image_type_slug") or slug_key

                    cursor.execute("""
                        INSERT INTO tank_images (emp_id, inspection_id, image_id, tank_number, image_type, image_path, created_at, created_date)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), CURDATE())
                    """, (emp_to_use, derived_insp_id, image_type_id, tank_number, final_slug, saved_info["image_path"]))
                    
                    successful_inserts.append({"image_type": type_info["image_type"], "filename": saved_info["image_path"]})

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
# ENDPOINT: Upload image (create) - requires Authorization header
# POST /api/upload/{inspection_id}/{image_type_id}
# ------------------------------------------------------------------
@router.post("/{inspection_id}/{image_type_id}", status_code=status.HTTP_201_CREATED)
async def upload_image(
    inspection_id: int,
    image_type_id: int,
    file: UploadFile = File(...),
    Authorization: Optional[str] = Header(None),
):
    try:
        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="File must be an image")

        # 1) Resolve inspection -> tank_number and legacy operator (fallback)
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(
                    "SELECT inspection_id, tank_number, operator_id FROM tank_inspection_details WHERE inspection_id=%s LIMIT 1",
                    (inspection_id,),
                )
                insp = cursor.fetchone()
        finally:
            conn.close()

        if not insp:
            raise HTTPException(status_code=404, detail="Invalid inspection_id")

        tank_number = insp.get("tank_number")
        legacy_emp = insp.get("operator_id") if insp.get("operator_id") is not None else None

        # 2) Get token subject and resolve emp_id from users table
        token_sub = _get_user_id_from_auth_header(Authorization)
        authenticated_emp_id = None
        if token_sub is not None:
            conn = get_db_connection()
            try:
                with conn.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT emp_id FROM users WHERE id = %s LIMIT 1", (token_sub,))
                    urow = cursor.fetchone()
                    if urow and urow.get("emp_id") is not None:
                        try:
                            authenticated_emp_id = int(urow.get("emp_id"))
                        except Exception:
                            authenticated_emp_id = None
            finally:
                conn.close()

        # choose emp_id: prefer authenticated_emp_id from users table, else fallback to legacy operator
        emp_to_use = authenticated_emp_id if authenticated_emp_id is not None else legacy_emp

        # 3) Validate image_type and compute slug
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT id, image_type FROM image_type WHERE id = %s LIMIT 1", (image_type_id,))
                it_row = cursor.fetchone()
        finally:
            conn.close()

        if not it_row:
            raise HTTPException(status_code=400, detail="Invalid image_type_id")

        resolved_label = it_row.get("image_type")
        resolved_slug = str(resolved_label).strip().lower().replace(" ", "")
        resolved_slug = "".join(ch for ch in resolved_slug if ch.isalnum() or ch in ("-", "_"))

        # 4) Save file to disk (uses your existing helper)
        saved_info = save_uploaded_file(file, tank_number, image_type_id)
        image_path = saved_info["image_path"]
        resolved_slug = saved_info.get("resolved_image_type_slug") or resolved_slug

        # 5) Insert row using emp_to_use (this will be value from users.emp_id when token provided)
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                try:
                    sql = """
                        INSERT INTO tank_images (emp_id, inspection_id, image_id, tank_number, image_type, image_path, created_at, created_date)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), CURDATE())
                    """
                    cursor.execute(sql, (
                        emp_to_use,
                        inspection_id,
                        image_type_id,
                        tank_number,
                        resolved_slug,
                        image_path,
                    ))
                    conn.commit()
                except Exception:
                    delete_file(image_path)
                    raise

                cursor.execute(
                    """
                    SELECT id, emp_id, inspection_id, image_id, tank_number, image_type, image_path, created_at, updated_at, created_date
                    FROM tank_images
                    WHERE inspection_id=%s AND image_id=%s
                    ORDER BY created_at ASC
                    """,
                    (inspection_id, image_type_id),
                )
                rows = cursor.fetchall()
        finally:
            conn.close()

        filename = image_path.split("/", 1)[1] if "/" in image_path else file.filename

        return {
            "success": True,
            "message": "Image uploaded successfully",
            "data": {
                "inspection_id": inspection_id,
                "image_type_id": image_type_id,
                "image_type": resolved_label,
                "image_type_slug": resolved_slug,
                "tank_number": tank_number,
                "images": rows,
                "filename": filename,
                "thumbnail": saved_info.get("thumbnail_path"),
                "uploaded_file_local_path": "/mnt/data/3ea6e84c-39d2-4320-bea1-06582b085acd.png"
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        try:
            if "image_path" in locals() and image_path:
                delete_file(image_path)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------------------------------------------
# ENDPOINT: Get images by inspection (GET) - unchanged (no auth)
# ------------------------------------------------------------------
@router.get("/images/inspection/{inspection_id}")
def get_images_by_inspection(inspection_id: int):
    try:
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT * FROM tank_images WHERE inspection_id = %s ORDER BY created_at ASC", (inspection_id,))
                rows = cursor.fetchall() or []
        finally:
            conn.close()

        if not rows:
            return {"success": True, "data": [], "message": f"No images found for inspection_id {inspection_id}"}

        enriched_rows = []
        for row in rows:
            thumb = None
            try:
                base = row.get("image_path")
                if base:
                    folder = os.path.dirname(base)
                    name = os.path.splitext(os.path.basename(base))[0]
                    folder_abs = os.path.join(UPLOAD_DIR, folder)
                    if os.path.isdir(folder_abs):
                        prefix = "_".join(name.split('_')[:2]) + "_"
                        for fn in os.listdir(folder_abs):
                            if fn.startswith(prefix) and fn.endswith('_thumb.jpg'):
                                thumb = f"{folder}/{fn}"
                                break
            except Exception:
                thumb = None

            row_with_thumb = dict(row)
            row_with_thumb["thumbnail_path"] = thumb
            enriched_rows.append(row_with_thumb)

        return {"success": True, "data": enriched_rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------------
# ENDPOINT: Replace image by id (PUT) - requires Authorization header
# ------------------------------------------------------------------

@router.put("/images/{id}")
async def replace_image_by_id(id: int, file: UploadFile = File(...), Authorization: Optional[str] = Header(None)):
    try:
        if not file.content_type or not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="File must be an image")

        # 1) fetch existing row
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute("SELECT * FROM tank_images WHERE id = %s", (id,))
                existing = cursor.fetchone()
                if not existing:
                    raise HTTPException(status_code=404, detail="Image id not found")

                tank_number = existing.get("tank_number")
                if not tank_number:
                    raise HTTPException(status_code=400, detail="Existing image row missing tank_number")

                image_type = existing.get("image_type") or "image"
        finally:
            conn.close()

        # 2) Save new file
        saved_info = save_uploaded_file(file, tank_number, image_type)
        new_path = saved_info["image_path"]

        # 3) Resolve authenticated user's emp_id
        token_sub = _get_user_id_from_auth_header(Authorization)
        if token_sub is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization required")

        authenticated_emp_id = None
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

        # 4) derive latest inspection_id for this tank
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                derived_insp_id = _derive_latest_inspection_id(cursor, tank_number)
        finally:
            conn.close()

        # 5) Update DB row
        conn = get_db_connection()
        try:
            with conn.cursor(DictCursor) as cursor:
                try:
                    cursor.execute(
                        "UPDATE tank_images SET image_path=%s, emp_id=%s, inspection_id=%s, updated_at=NOW() WHERE id=%s",
                        (new_path, authenticated_emp_id, derived_insp_id, id)
                    )
                    conn.commit()
                except Exception:
                    delete_file(new_path)
                    raise

                old_path = existing.get("image_path")
                if old_path:
                    delete_file(old_path)

                cursor.execute(
                    """
                    SELECT id, emp_id, inspection_id, image_id, tank_number, image_type, image_path, created_at, updated_at, created_date
                    FROM tank_images WHERE id = %s
                    """,
                    (id,)
                )
                image_row = cursor.fetchone()
        finally:
            conn.close()

        filename = new_path.split('/', 1)[1] if '/' in new_path else file.filename

        return {
            "success": True,
            "message": "Image replaced successfully",
            "data": {
                **(image_row or {}),
                "filename": filename,
                "thumbnail": saved_info.get("thumbnail_path"),
                "uploaded_file_local_path": UPLOADED_FILE_PATH
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        try:
            if "new_path" in locals() and new_path:
                delete_file(new_path)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))

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