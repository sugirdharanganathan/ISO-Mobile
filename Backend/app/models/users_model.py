from sqlalchemy import Column, Integer, String, DateTime, func
from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    emp_id = Column(Integer, nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    department = Column(String(100), nullable=True)
    designation = Column(String(100), nullable=True)
    hod = Column(String(100), nullable=True)
    supervisor = Column(String(100), nullable=True)
    email = Column(String(150), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)
    password_salt = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
