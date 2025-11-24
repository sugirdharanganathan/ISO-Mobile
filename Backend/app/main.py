# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import get_openapi
from fastapi import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.responses import JSONResponse
from fastapi import Request
import json
import logging

# import your routers and db init
from app.routers import tank_image_router, tank_inspection_router, auth_router
from app.routers.tank_checkpoints_router import router as tank_checkpoints_router
from app.routers import to_do_list_router
from app.database import init_db


# Initialize database
init_db()

# Create a single FastAPI app instance (do NOT create it twice)
app = FastAPI(title="ISO Tank API", version="1.0.0", description="ISO Tank API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(tank_image_router.router)
app.include_router(tank_checkpoints_router)
app.include_router(auth_router.router)
app.include_router(tank_inspection_router.router)
app.include_router(to_do_list_router.router)

# Serve uploaded images statically so frontend can fetch them
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

logger = logging.getLogger("uvicorn.error")


# Uniform response middleware: wrap JSON responses in the required envelope
class UniformResponseMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            response = await call_next(request)

            # Don't wrap docs, openapi, static files, uploads, or streaming responses
            path = request.url.path
            if path.startswith("/docs") or path.startswith("/redoc") or path.startswith("/openapi") or path.startswith("/static") or path.startswith("/uploads"):
                return response

            # Skip wrapping for streaming or file responses (indicated by streaming_body or non-JSON content-type)
            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                return response

            # Check if response is a streaming response
            if hasattr(response, "body_iterator") and response.body_iterator is not None:
                return response

            # For JSONResponse objects, we can safely access .body
            try:
                body_bytes = getattr(response, "body", None)
                if body_bytes is None:
                    return response
                body = json.loads(body_bytes.decode())
            except Exception:
                # If we can't parse it, return original response
                return response

            # If already in uniform format, return as-is
            if isinstance(body, dict) and set(("success", "message", "data")).issubset(body.keys()):
                return response

            # Wrap the original body as data
            wrapped = {"success": True, "message": "Operation successful", "data": body if body is not None else {}}
            return JSONResponse(content=wrapped, status_code=response.status_code)

        except Exception as exc:
            logger.exception("Error in UniformResponseMiddleware")
            return JSONResponse(content={"success": False, "message": "Internal server error", "data": {}}, status_code=500)


# attach middleware
app.add_middleware(UniformResponseMiddleware)


# Exception handlers to return uniform error shape
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # exc.detail may be dict or str
    msg = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return JSONResponse(content={"success": False, "message": msg or "Error", "data": {}}, status_code=exc.status_code)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(content={"success": False, "message": "Internal server error", "data": {}}, status_code=500)


@app.get("/")
def root():
    return {"message": "ISO Tank API is running"}


@app.get("/health")
def health_check():
    return {"status": "healthy"}


# -------------------------
# Custom OpenAPI (Bearer)
# -------------------------
def custom_openapi():
    # Return cached schema if already generated
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=getattr(app, "description", None),
        routes=app.routes,
    )

    # Add Bearer auth security scheme
    openapi_schema.setdefault("components", {}).setdefault("securitySchemes", {})
    openapi_schema["components"]["securitySchemes"]["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
    }

    # Optionally require it globally (makes the Authorize button appear)
    for path, path_item in openapi_schema.get("paths", {}).items():
        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue
            security = operation.setdefault("security", [])
            if {"BearerAuth": []} not in security:
                security.append({"BearerAuth": []})

    app.openapi_schema = openapi_schema
    return app.openapi_schema


# Attach custom openapi to the app so /docs shows the Authorize button
app.openapi = custom_openapi
