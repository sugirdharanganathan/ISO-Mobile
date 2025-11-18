import pymysql
from pymysql.cursors import DictCursor
import os
from dotenv import load_dotenv
import urllib.parse   # <- for percent-encoding password
load_dotenv()

# Database configuration (env defaults)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "iso_tank")
DB_PORT = int(os.getenv("DB_PORT", 3306))

# ---------------------------------------------------------------------------
# SQLAlchemy Configuration for ORM Models
# ---------------------------------------------------------------------------
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from typing import Generator

# Percent-encode password for URL (handles special chars like @, :, /)
password_enc = urllib.parse.quote_plus(DB_PASSWORD)

SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{password_enc}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,  # Test connection before using
)

# Create declarative base for ORM models
Base = declarative_base()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency for FastAPI endpoints to get a database session.
    Ensures the session is properly closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# pymysql direct helpers
# ---------------------------------------------------------------------------

def get_db_connection():
    """Create and return a database connection"""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=DictCursor,
        autocommit=False
    )


# ---------------------------------------------------------------------------
# init_db: Create database and seed master data (table creation via ORM)
# ---------------------------------------------------------------------------

def init_db():
    """Initialize database and seed master data"""
    # Create database if it doesn't exist
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursorclass=DictCursor
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            cursor.execute(f"USE {DB_NAME}")

            # Seed tank_status
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM tank_status")
                if cursor.fetchone()['cnt'] == 0:
                    tank_status_data = [
                        ('Laden', 'Tank is loaded / filled'),
                        ('Empty', 'Tank is empty'),
                        ('Residue', 'Only residue remains'),
                    ]
                    for status_name, description in tank_status_data:
                        cursor.execute(
                            "INSERT INTO tank_status (status_name, description) VALUES (%s, %s)",
                            (status_name, description)
                        )
            except Exception as e:
                print(f"Warning: Could not seed tank_status: {e}")

            # Seed product_master
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM product_master")
                if cursor.fetchone()['cnt'] == 0:
                    product_data = [
                        ('Liquid Argon', 'Cryogenic product - Liquid Argon'),
                        ('Liquid Carbon Dioxide', 'Cryogenic product - Liquid CO2'),
                        ('Liquid Oxygen', 'Cryogenic product - Liquid O2'),
                        ('Liquid Nitrogen', 'Cryogenic product - Liquid N2'),
                        ('Others', 'Other product - specified in notes'),
                    ]
                    for product_name, description in product_data:
                        cursor.execute(
                            "INSERT INTO product_master (product_name, description) VALUES (%s, %s)",
                            (product_name, description)
                        )
            except Exception as e:
                print(f"Warning: Could not seed product_master: {e}")

            # Seed inspection_type
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_type")
                if cursor.fetchone()['cnt'] == 0:
                    inspection_type_data = [
                        ('Incoming', 'Incoming inspection'),
                        ('Outgoing', 'Outgoing inspection'),
                        ('On-Hire', 'On-hire inspection'),
                        ('Off-Hire', 'Off-hire inspection'),
                        ('Condition', 'Condition check'),
                    ]
                    for it_name, description in inspection_type_data:
                        cursor.execute(
                            "INSERT INTO inspection_type (inspection_type_name, description) VALUES (%s, %s)",
                            (it_name, description)
                        )
            except Exception as e:
                print(f"Warning: Could not seed inspection_type: {e}")

            # Seed location_master
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM location_master")
                if cursor.fetchone()['cnt'] == 0:
                    location_data = [
                        ('SG-1 16A, Benoi Cresent', 'Default location'),
                        ('SG-2 5A Jalan Papan', 'Alternate location'),
                        ('China QD', 'China QD location'),
                    ]
                    for loc_name, description in location_data:
                        cursor.execute(
                            "INSERT INTO location_master (location_name, description) VALUES (%s, %s)",
                            (loc_name, description)
                        )
            except Exception as e:
                print(f"Warning: Could not seed location_master: {e}")

            # Seed inspection_status
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_status")
                if cursor.fetchone()['cnt'] == 0:
                    status_data = [
                        ('OK', 'All checks passed', 1),
                        ('FAULTY', 'Issues found that need attention', 2),
                        ('NA', 'Not applicable for this tank', 3),
                    ]
                    for status_name, description, sort_order in status_data:
                        cursor.execute(
                            "INSERT INTO inspection_status (status_name, description, sort_order) VALUES (%s, %s, %s)",
                            (status_name, description, sort_order)
                        )
            except Exception as e:
                print(f"Warning: Could not seed inspection_status: {e}")

            # Seed inspection_job
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_job")
                if cursor.fetchone()['cnt'] == 0:
                    job_data = [
                        ('J1', 'Tank & Frame Condition', 1),
                        ('J2', 'Pipework & Installation', 2),
                        ('J3', 'Tank Instrument & Assembly', 3),
                        ('J4', 'Valves Tightness & Operation', 4),
                        ('J5', 'Before Departure Check', 5),
                        ('J6', 'Others Observation & Comment', 6),
                    ]
                    for job_code, job_desc, sort_order in job_data:
                        cursor.execute(
                            "INSERT INTO inspection_job (job_code, job_description, sort_order) VALUES (%s, %s, %s)",
                            (job_code, job_desc, sort_order)
                        )
            except Exception as e:
                print(f"Warning: Could not seed inspection_job: {e}")

            # Seed inspection_sub_job
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_sub_job")
                if cursor.fetchone()['cnt'] == 0:
                    sub_job_data = [
                        (1, '1.1', 'Body x 6 Sides & All Frame – No Dent / No Bent / No Deep Cut', 1),
                        (1, '1.2', 'Cabin Door & Frame Condition – No Damage / Can Lock', 2),
                        (1, '1.3', 'Tank Number, Product & Hazchem Label – Not Missing or Tear', 3),
                        (1, '1.4', 'Condition of Paint Work & Cleanliness – Clean / No Bad Rust', 4),
                        (1, '1.5', 'Others', 5),
                        (2, '2.1', 'Pipework Supports / Brackets – Not Loose / No Bent', 1),
                        (2, '2.2', 'Pipework Joint & Welding – No Crack / No Icing / No Leaking', 2),
                        (2, '2.3', 'Earthing Point', 3),
                        (2, '2.4', 'PBU Support & Flange Connection – No Leak / Not Damage', 4),
                        (2, '2.5', 'Others', 5),
                        (3, '3.1', 'Safety Diverter Valve – Switching Lever', 1),
                        (3, '3.2', 'Safety Valves Connection & Joint – No Leaks', 2),
                        (3, '3.3', 'Level Gauge (mmHO) & Pressure Gauge (bar)', 3),
                        (3, '3.4', 'Level & Pressure Gauge Connection & Joint – No Leaks', 4),
                        (3, '3.5', 'Level & Pressure Gauge – Function Check', 5),
                        (3, '3.6', 'Level & Pressure Gauge Valve Open / Balance Valve Close', 6),
                        (3, '3.7', 'Vacuum Reading (micron)', 7),
                        (3, '3.8', 'Data & CSC Plate – Not Missing / Not Damage', 8),
                        (3, '3.9', 'Others', 9),
                        (4, '4.1', 'Valve Handwheel – Not Missing / Nut Not Loose', 1),
                        (4, '4.2', 'Valve Open & Close Operation – No Seizing / Not Tight / Not Jam', 2),
                        (4, '4.3', 'Valve Tightness Incl Glands – No Leak / No Icing / No Passing', 3),
                        (4, '4.4', 'Anchor Point', 4),
                        (4, '4.5', 'Others', 5),
                        (5, '5.1', 'All Valves Closed – Defrost & Close Firmly', 1),
                        (5, '5.2', 'Caps fitted to Outlets or Cover from Dust if applicable', 2),
                        (5, '5.3', 'Security Seal Fitted by Refilling Plant - Check', 3),
                        (5, '5.4', 'Pressure Gauge – lowest possible', 4),
                        (5, '5.5', 'Level Gauge – Within marking or standard indication', 5),
                        (5, '5.6', 'Weight Reading – ensure within acceptance weight', 6),
                        (5, '5.7', 'Cabin Door Lock – Secure and prevent from sudden opening', 7),
                        (5, '5.8', 'Others', 8),
                        (6, '6.1', 'Others Observation & Comment', 1),
                    ]
                    for job_id, sn, sub_job_desc, sort_order in sub_job_data:
                        cursor.execute(
                            "INSERT INTO inspection_sub_job (job_id, sn, sub_job_description, sort_order) VALUES (%s, %s, %s, %s)",
                            (job_id, sn, sub_job_desc, sort_order)
                        )
            except Exception as e:
                print(f"Warning: Could not seed inspection_sub_job: {e}")

            # Seed safety_valve_size
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM safety_valve_size")
                if cursor.fetchone()['cnt'] == 0:
                    sizes = ['1', '2.5', '3', '6', '2']
                    for val in sizes:
                        cursor.execute(
                            "INSERT INTO safety_valve_size (size_label, description) VALUES (%s, %s)",
                            (val, f"Size {val}")
                        )
            except Exception as e:
                print(f"Warning: Could not seed safety_valve_size: {e}")

            connection.commit()
    finally:
        connection.close()

    # Import all SQLAlchemy models and create tables via ORM
    model_modules = [
        "inspection_status_model",
        "tank_status_model",
        "product_master_model",
        "inspection_type_model",
        "location_master_model",
        "safety_valve_brand_model",
        "safety_valve_model_model",
        "safety_valve_size_model",
        "inspection_job_model",
        "inspection_sub_job_model",
        "inspection_report_model",
        "inspection_checklist_model",
        "tank_images_model",
        "users_model",
        "tank_inspection_details",
        "to_do_list_model",
    ]
    for mod in model_modules:
        try:
            __import__(f"app.models.{mod}", fromlist=["*"])
        except Exception as e:
            print(f"Warning: Could not import app.models.{mod}: {e}")

    # Create all tables via SQLAlchemy ORM
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: Base.metadata.create_all failed: {e}")

    # Ensure `lifter_weight` column exists on tank_inspection_details (safe-guard for environments)
    try:
        conn3 = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT, cursorclass=DictCursor)
        with conn3.cursor() as c:
            try:
                # MySQL supports ADD COLUMN IF NOT EXISTS in modern versions; fallback handled by catching exceptions
                c.execute("ALTER TABLE tank_inspection_details ADD COLUMN IF NOT EXISTS lifter_weight VARCHAR(255) NULL")
                conn3.commit()
            except Exception as e:
                # Try a safer approach: check information_schema and add if missing
                try:
                    c.execute("SELECT COUNT(*) as cnt FROM information_schema.columns WHERE table_schema=%s AND table_name='tank_inspection_details' AND column_name='lifter_weight'", (DB_NAME,))
                    cnt = c.fetchone().get('cnt', 0)
                    if cnt == 0:
                        c.execute("ALTER TABLE tank_inspection_details ADD COLUMN lifter_weight VARCHAR(255) NULL")
                        conn3.commit()
                except Exception as e2:
                    print(f"Warning: could not ensure lifter_weight column exists: {e2}")
    except Exception as e:
        print(f"Warning: could not connect to ensure lifter_weight column: {e}")
    finally:
        try:
            conn3.close()
        except Exception:
            pass

    # Ensure there is no UNIQUE index blocking multiple images per type (e.g. tank_number,image_type,created_date)
    try:
        conn2 = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT, cursorclass=DictCursor)
        with conn2.cursor() as c:
            # Find unique indexes on tank_images
            c.execute("SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols, NON_UNIQUE FROM information_schema.statistics WHERE table_schema=%s AND table_name='tank_images' GROUP BY INDEX_NAME", (DB_NAME,))
            indexes = c.fetchall()
            for idx in indexes:
                try:
                    cols = idx.get('cols') or ''
                    non_unique = idx.get('NON_UNIQUE')
                    name = idx.get('INDEX_NAME')
                    # If index is unique and contains tank_number,image_type,created_date (in any order), drop it
                    if non_unique == 0:
                        colset = set([cname.strip() for cname in cols.split(',')])
                        target = {'tank_number', 'image_type', 'created_date'}
                        if target.issubset(colset):
                            try:
                                c.execute(f"ALTER TABLE tank_images DROP INDEX `{name}`")
                                conn2.commit()
                                print(f"Dropped unique index {name} on tank_images to allow multiple images per type")
                            except Exception as e:
                                print(f"Warning: Could not drop index {name}: {e}")
                except Exception as e:
                    print(f"Warning while inspecting index {idx}: {e}")
    except Exception as e:
        print(f"Warning: could not inspect/drop tank_images indexes: {e}")
    finally:
        try:
            conn2.close()
        except Exception:
            pass


# Call init_db() on module import
init_db()
