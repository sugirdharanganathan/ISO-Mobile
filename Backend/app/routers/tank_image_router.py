from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from typing import Optional, Dict
import os
import uuid
from datetime import date, datetime
from app.database import get_db_connection

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

def save_uploaded_file(file: UploadFile, tank_number: str, image_type: str) -> str:
    """Save uploaded file and return the file path"""
    # Generate unique filename
    file_extension = os.path.splitext(file.filename)[1] if file.filename else ".jpg"
    unique_filename = f"{tank_number}_{image_type}_{uuid.uuid4().hex}{file_extension}"
    
    # Create tank-specific directory
    tank_dir = os.path.join(UPLOAD_DIR, tank_number)
    if not os.path.exists(tank_dir):
        os.makedirs(tank_dir)
    
    # Save file
    file_path = os.path.join(tank_dir, unique_filename)
    with open(file_path, "wb") as buffer:
        content = file.file.read()
        buffer.write(content)
    
    # Return relative path for database storage
    return os.path.join(tank_number, unique_filename)

def delete_file(file_path: str):
    """Delete a file from the filesystem"""
    try:
        full_path = os.path.join(UPLOAD_DIR, file_path)
        if os.path.exists(full_path):
            os.remove(full_path)
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
    emp_id: Optional[int] = Query(None, description="Employee ID who uploads the image")
):
    """Upload an image for a tank by image type (slug)"""
    try:
        validate_tank(tank_number)
        normalized_type = normalize_image_type(image_type)
        
        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")
        
        image_path = save_uploaded_file(file, tank_number, normalized_type)
        # upsert
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                sql = """
                    INSERT INTO tank_images (emp_id, tank_number, image_type, image_path, created_at, created_date)
                    VALUES (%s, %s, %s, %s, NOW(), CURDATE())
                    ON DUPLICATE KEY UPDATE image_path=VALUES(image_path), updated_at=CURRENT_TIMESTAMP, emp_id=VALUES(emp_id), created_at=VALUES(created_at), created_date=VALUES(created_date)
                """
                cursor.execute(sql, (emp_id, tank_number, normalized_type, image_path))
                connection.commit()
                cursor.execute("SELECT * FROM tank_images WHERE tank_number=%s AND image_type=%s", (tank_number, normalized_type))
                image_row = cursor.fetchone()
        finally:
            connection.close()
        
        return {
            "success": True,
            "message": "Image uploaded successfully",
            "data": {
                **image_row,
                "image_label": IMAGE_TYPES[normalized_type],
                "filename": file.filename
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
        # build existing map
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM tank_images WHERE tank_number = %s", (tank_number,))
                existing_images = cursor.fetchall()
        finally:
            connection.close()
        existing_map = {img["image_type"]: img for img in existing_images}
        
        def build_entry(slug: str):
            row = existing_map.get(slug)
            return {
                "id": row["id"] if row else None,
                "tank_number": tank_number,
                "image_type": slug,
                "image_label": IMAGE_TYPES[slug],
                "image_path": row["image_path"] if row else None,
                "created_at": row["created_at"] if row else None,
                "updated_at": row["updated_at"] if row else None,
                "uploaded": row is not None
            }
        
        if image_type:
            normalized_type = normalize_image_type(image_type)
            data = [build_entry(normalized_type)]
        else:
            data = [build_entry(slug) for slug in IMAGE_TYPES.keys()]
        
        return {
            "success": True,
            "data": data
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{tank_number}/{image_type}")
async def update_image(
    tank_number: str,
    image_type: str,
    file: UploadFile = File(...),
    emp_id: Optional[int] = Query(None, description="Employee ID who uploads the image")
):
    """Update/replace an image for a tank by image type"""
    try:
        validate_tank(tank_number)
        normalized_type = normalize_image_type(image_type)
        
        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")
        
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM tank_images WHERE tank_number=%s AND image_type=%s", (tank_number, normalized_type))
                existing = cursor.fetchone()
        finally:
            connection.close()
        if existing and existing.get("image_path"):
            delete_file(existing["image_path"])
        
        image_path = save_uploaded_file(file, tank_number, normalized_type)
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                sql = """
                    INSERT INTO tank_images (emp_id, tank_number, image_type, image_path, created_at, created_date)
                    VALUES (%s, %s, %s, %s, NOW(), CURDATE())
                    ON DUPLICATE KEY UPDATE image_path=VALUES(image_path), updated_at=CURRENT_TIMESTAMP, emp_id=VALUES(emp_id), created_at=VALUES(created_at), created_date=VALUES(created_date)
                """
                cursor.execute(sql, (emp_id, tank_number, normalized_type, image_path))
                connection.commit()
                cursor.execute("SELECT * FROM tank_images WHERE tank_number=%s AND image_type=%s", (tank_number, normalized_type))
                image_row = cursor.fetchone()
        finally:
            connection.close()
        
        return {
            "success": True,
            "message": "Image updated successfully",
            "data": {
                **image_row,
                "image_label": IMAGE_TYPES[normalized_type],
                "filename": file.filename
            }
        }
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
            with connection.cursor() as cursor:
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
                with connection.cursor() as cursor:
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
                with connection.cursor() as cursor:
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
                with connection.cursor() as cursor:
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
