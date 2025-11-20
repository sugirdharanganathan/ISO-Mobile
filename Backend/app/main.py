from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.routers import tank_image_router, tank_inspection_router, auth_router
from app.routers.tank_checkpoints_router import router as tank_checkpoints_router
from app.routers import to_do_list_router
from app.database import init_db

# Initialize database
init_db()

app = FastAPI(title="ISO Tank API", version="1.0.0")


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

@app.get("/")
def root():
    return {"message": "ISO Tank API is running"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

