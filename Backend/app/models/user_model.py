from sqlalchemy import Column, Integer, String, TIMESTAMP, func
from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    # Business employee id that we generate sequentially
    emp_id = Column(Integer, unique=True, nullable=False, index=True)

    name = Column(String(100), nullable=False)
    department = Column(String(100))
    designation = Column(String(100))
    hod = Column(String(100))
    supervisor = Column(String(100))

    email = Column(String(150), unique=True, nullable=False, index=True)

    # Stored credentials
    password_hash = Column(String(255), nullable=False)
    password_salt = Column(String(64), nullable=False)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


