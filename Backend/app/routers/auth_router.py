# auth_router.py
from fastapi import APIRouter, HTTPException, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from typing import Optional
from app.database import get_db_connection
import os
import hashlib
import binascii
import jwt
from datetime import datetime, timedelta

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Load JWT secret and settings from env
JWT_SECRET = os.getenv("JWT_SECRET", "change_this_in_production")
JWT_ALGORITHM = "HS256"
JWT_EXP_DAYS = int(os.getenv("JWT_EXP_DAYS", "1"))  # default 1 day

class RegisterRequest(BaseModel):
    name: str
    department: Optional[str] = None
    designation: Optional[str] = None
    hod: Optional[str] = None
    supervisor: Optional[str] = None
    email: EmailStr
    password: str
    role: Optional[str] = "user"   # NEW: accept role, default to "user"

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

def hash_password(password: str, salt: Optional[str] = None):
    """
    PBKDF2-HMAC-SHA256 password hashing with salt.
    Returns (hash, salt).
    """
    if salt is None:
        salt = binascii.hexlify(os.urandom(16)).decode()
    # PBKDF2-HMAC-SHA256 with 100k iterations
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return binascii.hexlify(dk).decode(), salt

def create_jwt_token(payload: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = payload.copy()
    if expires_delta is None:
        expires_delta = timedelta(days=JWT_EXP_DAYS)
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire, "iat": datetime.utcnow()})
    token = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
    # PyJWT returns str in recent versions
    return token


def _get_token_subject_from_header(authorization: Optional[str]) -> Optional[int]:
    """Decode Bearer token and return emp_id if present, else user_id."""
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    token = parts[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    # prefer emp_id, fallback to user_id
    for key in ("emp_id", "user_id", "id"):
        if key in payload:
            try:
                return int(payload[key])
            except Exception:
                try:
                    return int(str(payload[key]))
                except Exception:
                    continue
    return None

@router.post("/register")
def register_user(body: RegisterRequest):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM users WHERE email = %s", (body.email,))
            existing = cursor.fetchone()
            if existing:
                return JSONResponse(
                    status_code=409,
                    content={
                        "success": False,
                        "message": "User with same email already exists",
                        "data": {"email": body.email}
                    }
                )

            cursor.execute("SELECT COALESCE(MAX(emp_id), 1000) + 1 AS next_emp_id FROM users")
            next_emp_id = cursor.fetchone()["next_emp_id"]

            pwd_hash, salt = hash_password(body.password)

            # INSERT now includes role column (defaults to "user" if not provided)
            cursor.execute("""
                INSERT INTO users (emp_id, name, department, designation, hod, supervisor, email, password_hash, password_salt, role)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (next_emp_id, body.name, body.department, body.designation, body.hod, body.supervisor, body.email, pwd_hash, salt, body.role or "user"))
            connection.commit()
            user_id = cursor.lastrowid
            cursor.execute("""
                SELECT id, emp_id, name, department, designation, hod, supervisor, email, role, created_at, updated_at
                FROM users WHERE id = %s
            """, (user_id,))
            user = cursor.fetchone()
            return {
                "success": True,
                "message": "User registered successfully",
                "data": user
            }
    finally:
        connection.close()

@router.post("/login")
def login_user(body: LoginRequest):
    """
    Login logic with JWT token generation and the exact response shapes requested.
    - If user not found -> success: false, message: "Invalid user credentials or no user found"
    - If password incorrect -> success: false, message: "Password is not correct", data: { "email": ... }
    - On success -> success: true, message: "Login successful", data: { "email": ..., "token": "JWT_TOKEN_HERE" }
    """
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE email = %s", (body.email,))
            user = cursor.fetchone()
            if not user:
                # Invalid user / not found
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "message": "Invalid user credentials or no user found",
                        "data": {}
                    }
                )

            expected_hash = user.get("password_hash")
            salt = user.get("password_salt")
            if expected_hash is None or salt is None:
                # malformed user row (no password stored)
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "message": "Invalid user credentials or no user found",
                        "data": {}
                    }
                )

            pwd_hash, _ = hash_password(body.password, salt)
            if pwd_hash != expected_hash:
                # Password incorrect -> return structured error with email as requested
                return JSONResponse(
                    status_code=401,
                    content={
                        "success": False,
                        "message": "Password is not correct",
                        "data": {"email": body.email}
                    }
                )

            # Password correct -> create JWT
            token_payload = {
                "emp_id": user.get("emp_id"),
                "email": user.get("email"),
                "user_id": user.get("id")
            }
            token = create_jwt_token(token_payload)

            # -------------------------
            # IMPORTANT CHANGE:
            # Only insert a new login_sessions row if there is NO active session
            # (still_logged_in = 1) for this emp_id.
            # If an active session already exists, we skip inserting a duplicate.
            # -------------------------
            cursor.execute("""
                SELECT id FROM login_sessions
                WHERE emp_id = %s AND still_logged_in = 1
                LIMIT 1
            """, (user["emp_id"],))
            active_session = cursor.fetchone()

            if not active_session:
                # No active session found -> insert a new login session record
                cursor.execute("""
                    INSERT INTO login_sessions (emp_id, email, still_logged_in)
                    VALUES (%s, %s, 1)
                """, (user["emp_id"], user["email"]))
                connection.commit()
            else:
                # Active session exists: do not create another row.
                pass

            return {
                "success": True,
                "message": "Login successful",
                "data": {
                    "email": user["email"],
                    "token": token
                }
            }
    finally:
        connection.close()

@router.post("/logout")
def logout_user(authorization: Optional[str] = Header(None)):
    """Logout by Authorization header (Bearer token).
    Decode token to find emp_id (or user_id) and mark login_sessions still_logged_in=0 for that emp_id.
    """
    emp_id = _get_token_subject_from_header(authorization)
    if emp_id is None:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE login_sessions SET still_logged_in = 0
                WHERE emp_id = %s AND still_logged_in = 1
            """, (emp_id,))
            connection.commit()
            return {"success": True, "message": "Logged out"}
    finally:
        connection.close()

# -------------------------
# NEW: operators endpoint (minimal)
# -------------------------
@router.get("/operators")
def get_operators():
    """
    Return all users with role = 'operator'.
    Note: path is /api/auth/operators because this router uses prefix '/api/auth'.
    If you want /api/users/operators keep the function in users_router instead.
    """
    try:
        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT emp_id, name
                    FROM users
                    WHERE role = 'operator'
                    ORDER BY emp_id
                """)
                operators = cursor.fetchall() or []
        finally:
            connection.close()

        return {
            "success": True,
            "message": "Operators fetched successfully",
            "data": operators
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
