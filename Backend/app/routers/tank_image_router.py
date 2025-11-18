from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional, Dict
from pymysql.cursors import DictCursor
import os
import uuid
from datetime import date, datetime
from app.database import get_db_connection
try:
    from PIL import Image
except Exception:
    Image = None

# Max upload size in bytes (5 MB)
MAX_UPLOAD_SIZE = 5 * 1024 * 1024
# Thumbnail size
THUMBNAIL_SIZE = (200, 200)

router = APIRouter(prefix="/api/upload", tags=["upload"])

# Default image types (slug -> display name) for mobile
IMAGE_TYPES: Dict[str, str] = {
    "frontview": "Front View",
    "rearview": "Rear View",
    "topview": "Top View",
    "undersideview": "Underside View",
    "frontlhview": "Front LH View",
    "rearlhview": "Rear LH View",
    "frontrhview": "Front RH View",
    "rearrhview": "Rear RH View",
    "lhsideview": "LH Side View",
    "rhsideview": "RH Side View",
    "valvessectionview": "Valves Section View",
    "safetyvalve": "Safety Valve",
    "levelpressuregauge": "Level / Pressure Gauge",
    "vacuumreading": "Vacuum Reading"
}

# Create uploads directory if it doesn't exist
UPLOAD_DIR = "uploads"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

def save_uploaded_file(file: UploadFile, tank_number: str, image_type: str) -> dict:
    """Save uploaded file (streaming) with size limit and generate thumbnail if possible.
    Returns dict with keys: image_path, thumbnail_path (or None), size
    """
    # Generate unique filename
    file_extension = os.path.splitext(file.filename)[1] if file.filename else ".jpg"
    unique_filename = f"{tank_number}_{image_type}_{uuid.uuid4().hex}{file_extension}"

    # Create tank-specific directory
    tank_dir_fs = os.path.join(UPLOAD_DIR, tank_number)
    os.makedirs(tank_dir_fs, exist_ok=True)

    # Save file (filesystem path) streaming to enforce size limit
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
                    # remove partial file
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

    result = {"image_path": f"{tank_number}/{unique_filename}", "thumbnail_path": None, "size": total}

    # Generate thumbnail if PIL available
    if Image is not None:
        try:
            thumb_name = f"{tank_number}_{image_type}_{uuid.uuid4().hex}_thumb.jpg"
            thumb_path_fs = os.path.join(tank_dir_fs, thumb_name)
            with Image.open(file_path_fs) as img:
                img.thumbnail(THUMBNAIL_SIZE)
                img.convert("RGB").save(thumb_path_fs, format="JPEG")
            result["thumbnail_path"] = f"{tank_number}/{thumb_name}"
        except Exception as e:
            print(f"Warning: thumbnail generation failed for {file_path_fs}: {e}")

    return result

def delete_file(file_path: str):
    """Delete a file from the filesystem"""
    try:
        full_path = os.path.join(UPLOAD_DIR, *file_path.split("/"))
        if os.path.exists(full_path):
            os.remove(full_path)
        # also try to remove any thumbnail files in same folder matching base name
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

@router.get("/types")
def get_image_types():
    """Get list of available image types (for mobile app dropdown/selection)"""
    return {
        "success": True,
        "data": [{"slug": slug, "label": label} for slug, label in IMAGE_TYPES.items()]
    }

def validate_tank(tank_number: str):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM tank_header WHERE tank_number = %s", (tank_number,))
            if cursor.fetchone()["count"] == 0:
                raise HTTPException(status_code=404, detail="Tank not found")
    finally:
        connection.close()
    return True

def normalize_image_type(image_type: str) -> str:
    slug = image_type.strip().lower()
    if slug not in IMAGE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid image type: '{image_type}'. Must be one of: {', '.join(IMAGE_TYPES.keys())}"
        )
    return slug

@router.post("/{tank_number}/{image_type}")
async def upload_image(
    tank_number: str,
    image_type: str,
    file: UploadFile = File(...),
):
    """Upload an image for a tank by image type (slug). emp_id is derived from the latest tank_inspection_details.operator_id for the tank."""
    try:
        validate_tank(tank_number)
        normalized_type = normalize_image_type(image_type)

        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")

        saved_info = save_uploaded_file(file, tank_number, normalized_type)
        image_path = saved_info["image_path"]

        # Persist DB row (allow multiple rows per image_type)
        connection = get_db_connection()
        try:
            with connection.cursor(DictCursor) as cursor:
                # derive emp_id from most recent tank_inspection_details.operator_id for this tank
                cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE tank_number=%s ORDER BY inspection_date DESC LIMIT 1", (tank_number,))
                op = cursor.fetchone()
                derived_emp = op.get("operator_id") if op and op.get("operator_id") is not None else None

                try:
                    sql = """
                        INSERT INTO tank_images (emp_id, tank_number, image_type, image_path, created_at, created_date)
                        VALUES (%s, %s, %s, %s, NOW(), CURDATE())
                    """
                    cursor.execute(sql, (derived_emp, tank_number, normalized_type, image_path))
                    connection.commit()
                except Exception:
                    # On DB error, remove saved file to avoid orphan
                    delete_file(image_path)
                    raise

                # return all images for this tank+type
                cursor.execute("SELECT * FROM tank_images WHERE tank_number=%s AND image_type=%s ORDER BY created_at ASC", (tank_number, normalized_type))
                rows = cursor.fetchall()
        finally:
            connection.close()

        filename = image_path.split('/', 1)[1] if '/' in image_path else file.filename
        return {
            "success": True,
            "message": "Image uploaded successfully",
            "data": {
                "image_type": normalized_type,
                "image_label": IMAGE_TYPES[normalized_type],
                "images": rows,
                "filename": filename,
                "thumbnail": saved_info.get("thumbnail_path")
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{tank_number}/images")
def get_tank_images(tank_number: str, image_type: Optional[str] = None):
    """Get all images for a tank, optionally filtered by image type slug"""
    try:
        validate_tank(tank_number)

        # fetch all images for tank and group by type
        connection = get_db_connection()
        try:
            with connection.cursor(DictCursor) as cursor:
                cursor.execute("SELECT * FROM tank_images WHERE tank_number = %s ORDER BY created_at ASC", (tank_number,))
                existing_images = cursor.fetchall()
        finally:
            connection.close()

        grouped = {}
        for r in existing_images:
            grouped.setdefault(r["image_type"], []).append(r)

        def build_entry(slug: str):
            imgs = grouped.get(slug, [])
            enriched = []
            for it in imgs:
                thumb = None
                try:
                    base = it.get("image_path")
                    if base:
                        # Use the same naming pattern as upload: <tank_number>_<image_type>_<uuid>_thumb.jpg
                        folder = os.path.dirname(base)
                        name = os.path.splitext(os.path.basename(base))[0]
                        folder_abs = os.path.join(UPLOAD_DIR, folder)
                        if os.path.isdir(folder_abs):
                            # Find a file in the folder that starts with the tank/image_type prefix and ends with _thumb.jpg
                            prefix = name.split('_')[0] + '_' + name.split('_')[1] + '_'
                            for fn in os.listdir(folder_abs):
                                if fn.startswith(prefix) and fn.endswith('_thumb.jpg'):
                                    thumb = f"{folder}/{fn}"
                                    break
                except Exception:
                    thumb = None
                enriched.append({**it, "thumbnail_path": thumb})

            return {
                "tank_number": tank_number,
                "image_type": slug,
                "image_label": IMAGE_TYPES[slug],
                "images": enriched,
                "count": len(enriched),
                "uploaded": len(enriched) > 0
            }

        if image_type:
            normalized_type = normalize_image_type(image_type)
            data = [build_entry(normalized_type)]
        else:
            data = [build_entry(slug) for slug in IMAGE_TYPES.keys()]

        return {"success": True, "data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{tank_number}/{image_type}")
async def update_image(
    tank_number: str,
    image_type: str,
    file: UploadFile = File(...),
    image_id: Optional[int] = Query(None, description="If provided, replace this image id")
):
    """Update/replace an image for a tank by image type"""
    try:
        validate_tank(tank_number)
        normalized_type = normalize_image_type(image_type)
        
        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")
        
        # If image_id provided: replace that specific image row
        if image_id is not None:
            connection = get_db_connection()
            try:
                with connection.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT * FROM tank_images WHERE id=%s AND tank_number=%s", (image_id, tank_number))
                    existing = cursor.fetchone()
                    if not existing:
                        raise HTTPException(status_code=404, detail="Image id not found")
                    # save new file first
                    saved_info = save_uploaded_file(file, tank_number, normalized_type)
                    new_image_path = saved_info["image_path"]
                    try:
                        # derive emp_id from recent inspection details
                        cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE tank_number=%s ORDER BY inspection_date DESC LIMIT 1", (tank_number,))
                        op = cursor.fetchone()
                        derived_emp = op.get("operator_id") if op and op.get("operator_id") is not None else None
                        cursor.execute("UPDATE tank_images SET image_path=%s, emp_id=%s, updated_at=NOW() WHERE id=%s", (new_image_path, derived_emp, image_id))
                        connection.commit()
                    except Exception:
                        # cleanup file if DB update fails
                        delete_file(new_image_path)
                        raise
                    # remove old file
                    if existing.get("image_path"):
                        delete_file(existing["image_path"])
                    cursor.execute("SELECT * FROM tank_images WHERE id=%s", (image_id,))
                    image_row = cursor.fetchone()
            finally:
                connection.close()
            filename = new_image_path.split('/',1)[1] if '/' in new_image_path else file.filename
            return {"success": True, "message": "Image replaced successfully", "data": {**image_row, "image_label": IMAGE_TYPES[normalized_type], "filename": filename, "thumbnail": saved_info.get("thumbnail_path")}}

        # otherwise: insert as a new image (append)
        saved_info = save_uploaded_file(file, tank_number, normalized_type)
        image_path = saved_info["image_path"]
        connection = get_db_connection()
        try:
            with connection.cursor(DictCursor) as cursor:
                # derive emp_id from most recent inspection details
                cursor.execute("SELECT operator_id FROM tank_inspection_details WHERE tank_number=%s ORDER BY inspection_date DESC LIMIT 1", (tank_number,))
                op = cursor.fetchone()
                derived_emp = op.get("operator_id") if op and op.get("operator_id") is not None else None
                try:
                    cursor.execute("INSERT INTO tank_images (emp_id, tank_number, image_type, image_path, created_at, created_date) VALUES (%s, %s, %s, %s, NOW(), CURDATE())", (derived_emp, tank_number, normalized_type, image_path))
                    connection.commit()
                except Exception:
                    delete_file(image_path)
                    raise
                cursor.execute("SELECT * FROM tank_images WHERE tank_number=%s AND image_type=%s ORDER BY created_at ASC", (tank_number, normalized_type))
                rows = cursor.fetchall()
        finally:
            connection.close()
        filename = image_path.split('/',1)[1] if '/' in image_path else file.filename
        return {"success": True, "message": "Image added successfully", "data": {"image_type": normalized_type, "image_label": IMAGE_TYPES[normalized_type], "images": rows, "filename": filename, "thumbnail": saved_info.get("thumbnail_path")}}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{tank_number}/image")
def delete_specific_image(
    tank_number: str,
    image_type: str = Query(..., description="Image type slug"),
    date_str: str = Query(..., description="YYYY-MM-DD date of the image")
):
    """Delete a specific image by tank number, type, and date."""
    try:
        validate_tank(tank_number)
        normalized_type = normalize_image_type(image_type)
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        connection = get_db_connection()
        try:
            with connection.cursor(DictCursor) as cursor:
                cursor.execute(
                    "SELECT image_path FROM tank_images WHERE tank_number=%s AND image_type=%s AND created_date=%s",
                    (tank_number, normalized_type, date_str)
                )
                row = cursor.fetchone()
                cursor.execute(
                    "DELETE FROM tank_images WHERE tank_number=%s AND image_type=%s AND created_date=%s",
                    (tank_number, normalized_type, date_str)
                )
                connection.commit()
        finally:
            connection.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Image not found for the specified date")
        
        if row.get("image_path"):
            delete_file(row["image_path"])
        
        return {
            "success": True,
            "message": f"Image '{normalized_type}' deleted for {tank_number} on {date_str}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{tank_number}/image/{image_id}")
def delete_image_by_id(tank_number: str, image_id: int):
    """Delete a single image by its DB id and remove filesystem files (including thumbnail)."""
    try:
        validate_tank(tank_number)

        connection = get_db_connection()
        try:
            with connection.cursor(DictCursor) as cursor:
                cursor.execute("SELECT image_path FROM tank_images WHERE id=%s AND tank_number=%s", (image_id, tank_number))
                row = cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Image id not found for this tank")
                cursor.execute("DELETE FROM tank_images WHERE id=%s", (image_id,))
                connection.commit()
        finally:
            connection.close()

        # remove files from disk
        if row.get("image_path"):
            delete_file(row["image_path"])

        return {"success": True, "message": f"Image id {image_id} deleted", "deleted": 1}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{tank_number}/images")
def delete_tank_images(
    tank_number: str,
    date_str: Optional[str] = Query(None, description="YYYY-MM-DD; if omitted deletes images for today, 'all' deletes everything")
):
    """Delete images for a tank based on date (or all)."""
    try:
        validate_tank(tank_number)
        
        if date_str is None or date_str.lower() == "today":
            target_date = date.today().isoformat()
            connection = get_db_connection()
            try:
                with connection.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT image_path FROM tank_images WHERE tank_number=%s AND created_date=%s", (tank_number, target_date))
                    rows = cursor.fetchall()
                    cursor.execute("DELETE FROM tank_images WHERE tank_number=%s AND created_date=%s", (tank_number, target_date))
                    connection.commit()
                    paths = [r["image_path"] for r in rows]
            finally:
                connection.close()
            message = f"Images for {tank_number} deleted for {target_date}"
        elif date_str.lower() == "all":
            connection = get_db_connection()
            try:
                with connection.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT image_path FROM tank_images WHERE tank_number=%s", (tank_number,))
                    rows = cursor.fetchall()
                    cursor.execute("DELETE FROM tank_images WHERE tank_number=%s", (tank_number,))
                    connection.commit()
                    paths = [r["image_path"] for r in rows]
            finally:
                connection.close()
            message = "All images deleted successfully"
        else:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD, 'today', or 'all'.")
            connection = get_db_connection()
            try:
                with connection.cursor(DictCursor) as cursor:
                    cursor.execute("SELECT image_path FROM tank_images WHERE tank_number=%s AND created_date=%s", (tank_number, date_str))
                    rows = cursor.fetchall()
                    cursor.execute("DELETE FROM tank_images WHERE tank_number=%s AND created_date=%s", (tank_number, date_str))
                    connection.commit()
                    paths = [r["image_path"] for r in rows]
            finally:
                connection.close()
            message = f"Images for {tank_number} deleted for {date_str}"
        
        for path in paths:
            if path:
                delete_file(path)
        
        return {"success": True, "message": message, "deleted_count": len(paths)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
