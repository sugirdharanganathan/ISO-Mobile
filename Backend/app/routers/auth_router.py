from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from typing import Optional
from app.database import get_db_connection
import os
import hashlib
import binascii

router = APIRouter(prefix="/api/auth", tags=["auth"])

class RegisterRequest(BaseModel):
    name: str
    department: Optional[str] = None
    designation: Optional[str] = None
    hod: Optional[str] = None
    supervisor: Optional[str] = None
    email: EmailStr
    password: str

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

def hash_password(password: str, salt: Optional[str] = None):
    if salt is None:
        salt = binascii.hexlify(os.urandom(16)).decode()
    # PBKDF2-HMAC-SHA256
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return binascii.hexlify(dk).decode(), salt

@router.post("/register")
def register_user(body: RegisterRequest):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM users WHERE email = %s", (body.email,))
            existing = cursor.fetchone()
            if existing:
                raise HTTPException(status_code=409, detail="User with same email already exists")
            
            cursor.execute("SELECT COALESCE(MAX(emp_id), 1000) + 1 AS next_emp_id FROM users")
            next_emp_id = cursor.fetchone()["next_emp_id"]
            
            pwd_hash, salt = hash_password(body.password)
            cursor.execute("""
                INSERT INTO users (emp_id, name, department, designation, hod, supervisor, email, password_hash, password_salt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (next_emp_id, body.name, body.department, body.designation, body.hod, body.supervisor, body.email, pwd_hash, salt))
            connection.commit()
            user_id = cursor.lastrowid
            cursor.execute("""
                SELECT id, emp_id, name, department, designation, hod, supervisor, email, created_at, updated_at
                FROM users WHERE id = %s
            """, (user_id,))
            user = cursor.fetchone()
            return {"success": True, "message": "User registered successfully", "data": user}
    finally:
        connection.close()

@router.post("/login")
def login_user(body: LoginRequest):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE email = %s", (body.email,))
            user = cursor.fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="Invalid credentials")
            expected_hash = user["password_hash"]
            salt = user["password_salt"]
            pwd_hash, _ = hash_password(body.password, salt)
            if pwd_hash != expected_hash:
                raise HTTPException(status_code=401, detail="Invalid credentials")
            # Record session
            cursor.execute("""
                INSERT INTO login_sessions (emp_id, email, still_logged_in)
                VALUES (%s, %s, 1)
            """, (user["emp_id"], user["email"]))
            connection.commit()
            # Response shows only email and password as requested
            return {
                "success": True,
                "message": "Login successful",
                "data": {
                    "email": user["email"],
                    "password": body.password
                }
            }
    finally:
        connection.close()

@router.post("/logout")
def logout_user(email: EmailStr):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE login_sessions SET still_logged_in = 0
                WHERE email = %s AND still_logged_in = 1
            """, (email,))
            connection.commit()
            return {"success": True, "message": "Logged out"}
    finally:
        connection.close()


