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
# IMPORTANT: Engine and Base must be defined before init_db() is called
# (main.py calls init_db() during import). That was causing errors.
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
# pymysql direct helpers (used by init_db and lower-level operations)
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
# init_db: create DB, create/alter tables, and seed master data.
# ---------------------------------------------------------------------------

def init_db():
    """Initialize database and create tables if they don't exist"""
    # First connect without database to create it if needed
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursorclass=DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            cursor.execute(f"USE {DB_NAME}")

            # -----------------------------------------------------------------
            # Safety valve size master seeding (fixed set of sizes)
            # Keep the safety_valve_size table values as requested (1, 2.5, 3, 6, 2)
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema=%s AND table_name='safety_valve_size'", (DB_NAME,))
                if cursor.fetchone()['cnt'] > 0:
                    cursor.execute("DELETE FROM safety_valve_size")
                    sizes = ['1', '2.5', '3', '6', '2']
                    for val in sizes:
                        cursor.execute(
                            "INSERT INTO safety_valve_size (size_label, description) VALUES (%s, %s)",
                            (val, f"Size {val}")
                        )
            except Exception as e:
                # non-fatal
                pass

            # -----------------------------------------------------------------
            # Create tank_images table with new structure
            # -----------------------------------------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tank_images (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    emp_id INT NULL,
                    tank_number VARCHAR(50) NOT NULL,
                    image_type VARCHAR(50) NOT NULL,
                    image_path VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_date DATE GENERATED ALWAYS AS (DATE(created_at)) STORED,
                    UNIQUE KEY uq_tank_image (tank_number, image_type, created_date)
                )
            """)

            # -----------------------------------------------------------------
            # Create additional master tables required by the application
            # -----------------------------------------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inspection_status (
                    status_id INT AUTO_INCREMENT PRIMARY KEY,
                    status_name VARCHAR(32) NOT NULL UNIQUE,
                    description VARCHAR(255) DEFAULT NULL,
                    sort_order INT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tank_status (
                    status_id INT AUTO_INCREMENT PRIMARY KEY,
                    status_name VARCHAR(150) NOT NULL UNIQUE,
                    description TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product_master (
                    product_id INT AUTO_INCREMENT PRIMARY KEY,
                    product_name VARCHAR(150) NOT NULL UNIQUE,
                    description TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inspection_type (
                    inspection_type_id INT AUTO_INCREMENT PRIMARY KEY,
                    inspection_type_name VARCHAR(150) NOT NULL UNIQUE,
                    description TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS location_master (
                    location_id INT AUTO_INCREMENT PRIMARY KEY,
                    location_name VARCHAR(255) NOT NULL,
                    description TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # -----------------------------------------------------------------
            # Create safety valve master tables (brand, model, size)
            # These are independent master lists used by inspections
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS safety_valve_brand (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        brand_name VARCHAR(255) NOT NULL UNIQUE,
                        description TEXT DEFAULT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
            except Exception:
                pass

            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS safety_valve_model (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        model_name VARCHAR(255) NOT NULL UNIQUE,
                        description TEXT DEFAULT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
            except Exception:
                pass

            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS safety_valve_size (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        size_label VARCHAR(255) NOT NULL UNIQUE,
                        description TEXT DEFAULT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
            except Exception:
                pass

            # -----------------------------------------------------------------
            # Seed master data
            # -----------------------------------------------------------------
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

            # -----------------------------------------------------------------
            # Create inspection_report and inspection_checklist tables
            # -----------------------------------------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inspection_report (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    tank_number VARCHAR(50) NOT NULL,
                    inspection_date DATE NOT NULL,
                    emp_id INT NULL,
                    notes TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_report_tank_date (tank_number, inspection_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inspection_checklist (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_id INT NOT NULL,
                    tank_number VARCHAR(50) NULL,
                    sn VARCHAR(16) NOT NULL,
                    status_id INT NOT NULL DEFAULT 1,
                    status VARCHAR(32) NULL,
                    comment TEXT DEFAULT NULL,
                    photo_path VARCHAR(512) DEFAULT NULL,
                    flagged BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_report_sn (report_id, sn),
                    CONSTRAINT fk_checklist_report FOREIGN KEY (report_id) REFERENCES inspection_report(id) ON DELETE CASCADE,
                    CONSTRAINT fk_checklist_status FOREIGN KEY (status_id) REFERENCES inspection_status(status_id) ON DELETE RESTRICT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # Create to_do_list table (stores flagged items)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS to_do_list (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    checklist_id INT NOT NULL UNIQUE,
                    report_id INT NOT NULL,
                    tank_number VARCHAR(50) NOT NULL,
                    job_name VARCHAR(255) NULL,
                    sub_job_description VARCHAR(512) NULL,
                    sn VARCHAR(16) NOT NULL,
                    status VARCHAR(32) NULL,
                    comment TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_checklist_id (checklist_id),
                    INDEX idx_tank_number (tank_number),
                    CONSTRAINT fk_todo_checklist FOREIGN KEY (checklist_id) REFERENCES inspection_checklist(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # -----------------------------------------------------------------
            # Create inspection_job and inspection_sub_job
            # -----------------------------------------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inspection_job (
                    job_id INT AUTO_INCREMENT PRIMARY KEY,
                    job_code VARCHAR(32) NULL,
                    job_description VARCHAR(255) NOT NULL,
                    sort_order INT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inspection_sub_job (
                    sub_job_id INT AUTO_INCREMENT PRIMARY KEY,
                    job_id INT NOT NULL,
                    sn VARCHAR(16) NOT NULL,
                    sub_job_description VARCHAR(512) NOT NULL,
                    sort_order INT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_sn (sn),
                    CONSTRAINT fk_subjob_job FOREIGN KEY (job_id) REFERENCES inspection_job(job_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # -----------------------------------------------------------------
            # Create users table
            # -----------------------------------------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    emp_id INT NOT NULL UNIQUE,
                    name VARCHAR(100) NOT NULL,
                    department VARCHAR(100),
                    designation VARCHAR(100),
                    hod VARCHAR(100),
                    supervisor VARCHAR(100),
                    email VARCHAR(150) NOT NULL UNIQUE,
                    password_hash VARCHAR(255) NOT NULL,
                    password_salt VARCHAR(64) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)

            # -----------------------------------------------------------------
            # Alter inspection_checklist to add job_id, job_name, sub_job_description columns if not present
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='inspection_checklist' AND column_name='job_id'
                """, (DB_NAME,))
                if cursor.fetchone()['cnt'] == 0:
                    cursor.execute("""
                        ALTER TABLE inspection_checklist
                        ADD COLUMN job_id INT NULL AFTER report_id,
                        ADD COLUMN job_name VARCHAR(255) NULL AFTER job_id,
                        ADD COLUMN sub_job_description VARCHAR(512) NULL AFTER job_name,
                        ADD CONSTRAINT fk_checklist_job FOREIGN KEY (job_id) REFERENCES inspection_job(job_id) ON DELETE SET NULL
                    """)
            except Exception as e:
                print(f"Warning: Could not alter inspection_checklist: {e}")

            # -----------------------------------------------------------------
            # Drop legacy columns if they exist (non-fatal)
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='inspection_checklist' AND column_name='section'
                """, (DB_NAME,))
                if cursor.fetchone()['cnt'] > 0:
                    cursor.execute("ALTER TABLE inspection_checklist DROP COLUMN section")
            except Exception as e:
                print(f"Warning: Could not drop section column: {e}")

            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='inspection_checklist' AND column_name='job_description'
                """, (DB_NAME,))
                if cursor.fetchone()['cnt'] > 0:
                    cursor.execute("ALTER TABLE inspection_checklist DROP COLUMN job_description")
            except Exception as e:
                print(f"Warning: Could not drop job_description column: {e}")

            # -----------------------------------------------------------------
            # Seed inspection_job and inspection_sub_job master data if table is empty
            # -----------------------------------------------------------------
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

            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_sub_job")
                if cursor.fetchone()['cnt'] == 0:
                    sub_job_data = [
                        # Section 1: Tank & Frame Condition (job_id=1)
                        (1, '1.1', 'Body x 6 Sides & All Frame – No Dent / No Bent / No Deep Cut', 1),
                        (1, '1.2', 'Cabin Door & Frame Condition – No Damage / Can Lock', 2),
                        (1, '1.3', 'Tank Number, Product & Hazchem Label – Not Missing or Tear', 3),
                        (1, '1.4', 'Condition of Paint Work & Cleanliness – Clean / No Bad Rust', 4),
                        (1, '1.5', 'Others', 5),
                        # Section 2: Pipework & Installation (job_id=2)
                        (2, '2.1', 'Pipework Supports / Brackets – Not Loose / No Bent', 1),
                        (2, '2.2', 'Pipework Joint & Welding – No Crack / No Icing / No Leaking', 2),
                        (2, '2.3', 'Earthing Point', 3),
                        (2, '2.4', 'PBU Support & Flange Connection – No Leak / Not Damage', 4),
                        (2, '2.5', 'Others', 5),
                        # Section 3: Tank Instrument & Assembly (job_id=3)
                        (3, '3.1', 'Safety Diverter Valve – Switching Lever', 1),
                        (3, '3.2', 'Safety Valves Connection & Joint – No Leaks', 2),
                        (3, '3.3', 'Level Gauge (mmH₂O) & Pressure Gauge (bar)', 3),
                        (3, '3.4', 'Level & Pressure Gauge Connection & Joint – No Leaks', 4),
                        (3, '3.5', 'Level & Pressure Gauge – Function Check', 5),
                        (3, '3.6', 'Level & Pressure Gauge Valve Open / Balance Valve Close', 6),
                        (3, '3.7', 'Vacuum Reading (micron)', 7),
                        (3, '3.8', 'Data & CSC Plate – Not Missing / Not Damage', 8),
                        (3, '3.9', 'Others', 9),
                        # Section 4: Valves Tightness & Operation (job_id=4)
                        (4, '4.1', 'Valve Handwheel – Not Missing / Nut Not Loose', 1),
                        (4, '4.2', 'Valve Open & Close Operation – No Seizing / Not Tight / Not Jam', 2),
                        (4, '4.3', 'Valve Tightness Incl Glands – No Leak / No Icing / No Passing', 3),
                        (4, '4.4', 'Anchor Point', 4),
                        (4, '4.5', 'Others', 5),
                        # Section 5: Before Departure Check (job_id=5)
                        (5, '5.1', 'All Valves Closed – Defrost & Close Firmly', 1),
                        (5, '5.2', 'Caps fitted to Outlets or Cover from Dust if applicable', 2),
                        (5, '5.3', 'Security Seal Fitted by Refilling Plant - Check', 3),
                        (5, '5.4', 'Pressure Gauge – lowest possible', 4),
                        (5, '5.5', 'Level Gauge – Within marking or standard indication', 5),
                        (5, '5.6', 'Weight Reading – ensure within acceptance weight', 6),
                        (5, '5.7', 'Cabin Door Lock – Secure and prevent from sudden opening', 7),
                        (5, '5.8', 'Others', 8),
                        # Section 6: Others Observation & Comment (job_id=6)
                        (6, '6.1', 'Others Observation & Comment', 1),
                    ]
                    for job_id, sn, sub_job_desc, sort_order in sub_job_data:
                        cursor.execute(
                            "INSERT INTO inspection_sub_job (job_id, sn, sub_job_description, sort_order) VALUES (%s, %s, %s, %s)",
                            (job_id, sn, sub_job_desc, sort_order)
                        )
            except Exception as e:
                print(f"Warning: Could not seed inspection_sub_job: {e}")

            # -----------------------------------------------------------------
            # Seed inspection_status master data if table is empty
            # -----------------------------------------------------------------
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

            # -----------------------------------------------------------------
            # Add status_id and tank_number columns to inspection_checklist if not present
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='inspection_checklist' AND column_name='status_id'
                """, (DB_NAME,))
                if cursor.fetchone()['cnt'] == 0:
                    cursor.execute("""
                        ALTER TABLE inspection_checklist
                        ADD COLUMN status_id INT NOT NULL DEFAULT 1 AFTER sn,
                        ADD COLUMN tank_number VARCHAR(50) NULL AFTER report_id,
                        ADD CONSTRAINT fk_checklist_status FOREIGN KEY (status_id) REFERENCES inspection_status(status_id) ON DELETE RESTRICT
                    """)
            except Exception as e:
                print(f"Warning: Could not alter inspection_checklist to add status_id and tank_number: {e}")

            # Add status VARCHAR column if it doesn't exist (to store status name alongside status_id)
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.columns
                    WHERE table_schema=%s AND table_name='inspection_checklist' AND column_name='status'
                """, (DB_NAME,))
                if cursor.fetchone()['cnt'] == 0:
                    cursor.execute("""
                        ALTER TABLE inspection_checklist
                        ADD COLUMN status VARCHAR(32) NULL AFTER status_id
                    """)
            except Exception as e:
                print(f"Warning: Could not alter inspection_checklist to add status VARCHAR column: {e}")

            # Add inspection_id and job_id columns to inspection_checklist if not present
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
                    WHERE table_schema=%s AND table_name='inspection_checklist' AND column_name='inspection_id'
                """, (DB_NAME,))
                if cursor.fetchone()['cnt'] == 0:
                    cursor.execute("""
                        ALTER TABLE inspection_checklist
                        ADD COLUMN inspection_id INT NULL AFTER tank_number,
                        ADD COLUMN job_id INT NULL AFTER sn,
                        ADD COLUMN job_name VARCHAR(255) NULL,
                        ADD COLUMN sub_job_description VARCHAR(512) NULL
                    """)
            except Exception as e:
                print(f"Warning: Could not alter inspection_checklist to add inspection_id and job fields: {e}")

            # -----------------------------------------------------------------
            # Migrate users.emp_id to INT if an older schema used VARCHAR
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    SELECT DATA_TYPE 
                    FROM information_schema.COLUMNS
                    WHERE table_schema = %s AND table_name = 'users' AND column_name = 'emp_id'
                """, (DB_NAME,))
                dt_row = cursor.fetchone()
                if dt_row and dt_row.get("DATA_TYPE", "").lower() != "int":
                    try:
                        cursor.execute("ALTER TABLE users MODIFY emp_id INT NOT NULL")
                    except Exception as e:
                        print(f"Warning: Could not alter users.emp_id to INT: {e}")
            except Exception:
                # table may not exist yet - ignore
                pass

            # Ensure UNIQUE index on users.emp_id
            try:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM information_schema.statistics
                    WHERE table_schema=%s AND table_name='users' AND index_name='emp_id' AND NON_UNIQUE=0
                """, (DB_NAME,))
                idx_row = cursor.fetchone()
                if not idx_row or idx_row["cnt"] == 0:
                    try:
                        cursor.execute("CREATE UNIQUE INDEX emp_id ON users(emp_id)")
                    except Exception:
                        pass
            except Exception:
                pass

            # -----------------------------------------------------------------
            # Create login_sessions table to track logins
            # -----------------------------------------------------------------
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS login_sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    emp_id INT NOT NULL,
                    email VARCHAR(150) NOT NULL,
                    logged_in_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    still_logged_in TINYINT(1) DEFAULT 1,
                    CONSTRAINT fk_login_user_emp
                        FOREIGN KEY (emp_id) REFERENCES users(emp_id)
                        ON DELETE CASCADE
                )
            """)

            # Ensure foreign key on tank_images.emp_id -> users.emp_id (best-effort)
            try:
                cursor.execute("""
                    ALTER TABLE tank_images
                    ADD CONSTRAINT fk_tank_images_user_emp FOREIGN KEY (emp_id) REFERENCES users(emp_id) ON DELETE SET NULL
                """)
            except Exception:
                pass

            # -----------------------------------------------------------------
            # Try to add foreign key constraint if tank_header exists (for tank_images)
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM information_schema.table_constraints
                    WHERE table_schema = %s
                    AND table_name = 'tank_images'
                    AND constraint_type = 'FOREIGN KEY'
                    AND constraint_name LIKE '%%tank_number%%'
                """, (DB_NAME,))
                fk_exists = cursor.fetchone()['count'] > 0

                if not fk_exists:
                    # Check if tank_header exists and has proper index
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM information_schema.statistics
                        WHERE table_schema = %s 
                        AND table_name = 'tank_header' 
                        AND column_name = 'tank_number'
                    """, (DB_NAME,))
                    has_index = cursor.fetchone()['count'] > 0

                    if has_index:
                        try:
                            cursor.execute("""
                                ALTER TABLE tank_images
                                ADD CONSTRAINT fk_tank_images_tank_header
                                FOREIGN KEY (tank_number) REFERENCES tank_header(tank_number) ON DELETE CASCADE
                            """)
                        except Exception as e:
                            print(f"Warning: Could not add fk_tank_images_tank_header: {e}")
            except Exception as e:
                print(f"Warning: Could not add foreign key constraint for tank_images: {e}")

            # -----------------------------------------------------------------
            # Note: We do NOT enforce a strict FK constraint on inspection_report.tank_number -> tank_header.tank_number
            # because the tank may not exist yet when creating a report. Instead, we validate at the application level
            # in the checklist_router.py create_or_get_report() endpoint.
            # -----------------------------------------------------------------

            # -----------------------------------------------------------------
            # Try to add FK to users.emp_id on inspection_report (if users table exists)
            # -----------------------------------------------------------------
            try:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM information_schema.table_constraints
                    WHERE table_schema = %s
                    AND table_name = 'inspection_report'
                    AND constraint_type = 'FOREIGN KEY'
                    AND constraint_name LIKE '%%report_user%%'
                """, (DB_NAME,))
                fk_exists = cursor.fetchone()['count'] > 0
                if not fk_exists:
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = 'users'
                    """, (DB_NAME,))
                    users_exists = cursor.fetchone()['count'] > 0
                    if users_exists:
                        try:
                            cursor.execute("""
                                ALTER TABLE inspection_report
                                ADD CONSTRAINT fk_report_user
                                FOREIGN KEY (emp_id) REFERENCES users(emp_id) ON DELETE SET NULL
                            """)
                        except Exception:
                            pass
            except Exception as e:
                print(f"Warning: Could not add inspection_report user FK: {e}")

            # No final id-mapping pass for tank_mobile: table intentionally removed

            # Commit DDL/DML changes made in init_db
            try:
                connection.commit()
            except Exception as e:
                print(f"Warning: commit failed: {e}")
    finally:
        connection.close()

    # ---------------------------------------------------------------------
    # Ensure SQLAlchemy models are imported so SQLAlchemy knows about them.
    # Import individual model modules and log warnings but do not abort.
    # ---------------------------------------------------------------------
    model_modules = [
        "tank_inspection_details",
        "to_do_list_model",
    ]
    for mod in model_modules:
        try:
            __import__(f"app.models.{mod}", fromlist=["*"])
        except Exception as e:
            print(f"Warning: Could not import app.models.{mod}: {e}")

    # ---------------------------------------------------------------------
    # Create all tables for imported models (best-effort)
    # ---------------------------------------------------------------------
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: Base.metadata.create_all failed: {e}")
