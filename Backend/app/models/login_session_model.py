from sqlalchemy import Column, Integer, String, TIMESTAMP, Boolean, func
from app.database import Base


class LoginSession(Base):
    __tablename__ = "login_sessions"

    id = Column(Integer, primary_key=True, index=True)
    emp_id = Column(Integer, nullable=False, index=True)
    email = Column(String(150), nullable=False, index=True)
    logged_in_at = Column(TIMESTAMP, server_default=func.now())
    still_logged_in = Column(Boolean, default=True)


