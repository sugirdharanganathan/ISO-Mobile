from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Any

def success_resp(message: str, data: Any = None, status_code: int = 200):
    """
    Standardized Success Response
    """
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder({
            "success": True,
            "message": message,
            "data": data
        })
    )

def error_resp(message: str, status_code: int = 500):
    """
    Standardized Error Response
    """
    return JSONResponse(
        status_code=status_code,
        content=jsonable_encoder({
            "success": False,
            "message": message,
            "data": None
        })
    )