# app/models/operators_model.py
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base

class Operator(Base):
    """
    Operators table:
    - id: auto-increment PK
    - operator_id: emp_id from users table (FK -> users.emp_id)
    - operator_name: operator's name
    """
    __tablename__ = "operators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operator_id = Column(Integer, ForeignKey("users.emp_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, index=True)
    operator_name = Column(String(255), nullable=False)

    created_at = Column(DateTime(), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<Operator id={self.id} operator_id={self.operator_id} name={self.operator_name}>"
