from sqlalchemy import Column, Integer, String, Float, Date, DateTime, func, UniqueConstraint
from app.database import Base


class TankDetails(Base):
    __tablename__ = "tank_details"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tank_id = Column(Integer, nullable=False, unique=True)
    tank_number = Column(String(50), nullable=False, unique=True)
    status = Column(String(50), nullable=True)
    mfgr = Column(String(100), nullable=True)
    date_mfg = Column(Date, nullable=True)
    pv_code = Column(String(50), nullable=True)
    un_iso_code = Column(String(50), nullable=True)
    capacity_l = Column(Integer, nullable=True)
    mawp = Column(Float, nullable=True)
    design_temperature = Column(Float, nullable=True)
    tare_weight_kg = Column(Integer, nullable=True)
    mgw_kg = Column(Integer, nullable=True)
    mpl_kg = Column(Integer, nullable=True)
    size = Column(String(100), nullable=True)
    pump_type = Column(String(50), nullable=True)
    vesmat = Column(String(50), nullable=True)
    gross_kg = Column(Integer, nullable=True)
    net_kg = Column(Integer, nullable=True)
    color_body_frame = Column(String(50), nullable=True)
    working_pressure = Column(Float, nullable=True)
    cabinet_type = Column(String(50), nullable=True)
    frame_type = Column(String(50), nullable=True)
    remark = Column(String(255), nullable=True)
    lease = Column(String(50), nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_by = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('tank_id', name='uq_tank_details_tank_id'),
        UniqueConstraint('tank_number', name='uq_tank_details_tank_number'),
    )
