from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db
from app.utils import success_resp, error_resp 

router = APIRouter(prefix="/api/tank_checkpoints", tags=["Checkpoints"])

@router.get("/export/checklist")
def get_checklist_template(db: Session = Depends(get_db)):
    try:
        # ---------------------------------------------------------
        # SQL: Select IDs using ALIASES (Fixes the "Unknown column" error)
        # ---------------------------------------------------------
        # 1. We select 'j.id' but rename it to 'job_id' for Python
        # 2. We select 's.id' but rename it to 'sub_job_id' for Python
        # 3. We join on 'j.id' because that is the real column name
        query = """
            SELECT 
                j.id AS job_id, 
                j.job_name, 
                s.sub_job_id AS sub_job_id, 
                s.sub_job_name
            FROM inspection_job j
            LEFT JOIN inspection_sub_job s ON j.id = s.job_id
            ORDER BY j.id ASC, s.sub_job_id ASC
        """
        
        results = db.execute(text(query)).fetchall()

        sections_map = {}

        for row in results:
            # Convert row to dictionary
            r = dict(row._mapping) if hasattr(row, "_mapping") else dict(zip(row.keys(), row))
            
            # Now r['job_id'] works because we aliased it in the SQL above
            jid = str(r['job_id'])
            
            # 1. Create Section (With job_id included)
            if jid not in sections_map:
                sections_map[jid] = {
                    "sn": jid,               
                    "job_id": jid,           
                    "title": r['job_name'],
                    "items": []
                }
            
            # 2. Add Item (With job_id and sub_job_id included)
            if r['sub_job_id']:
                current_count = len(sections_map[jid]["items"]) + 1
                sn_formatted = f"{jid}.{current_count}"
                
                sections_map[jid]["items"].append({
                    "sn": sn_formatted,
                    "title": r['sub_job_name'],
                    "job_id": jid,                     
                    "sub_job_id": str(r['sub_job_id']) 
                })

        final_sections = list(sections_map.values())

        response_data = {
            "sections": final_sections
        }

        return success_resp("Checklist fetched successfully", response_data, 200)

    except Exception as e:
        return error_resp(f"Error fetching checklist: {str(e)}", 500)