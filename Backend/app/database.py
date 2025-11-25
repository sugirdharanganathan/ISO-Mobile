import pymysql
from pymysql.cursors import DictCursor
import os
from dotenv import load_dotenv
import urllib.parse
import traceback
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
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from typing import Generator
import importlib
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

password_enc = urllib.parse.quote_plus(DB_PASSWORD)
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{password_enc}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# pymysql direct helpers
# ---------------------------------------------------------------------------

def get_db_connection(use_db=True):
    """Create and return a database connection"""
    conn_params = dict(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursorclass=DictCursor,
        autocommit=False
    )
    if use_db:
        conn_params["database"] = DB_NAME
    return pymysql.connect(**conn_params)


# ---------------------------------------------------------------------------
# OPTIONAL: local uploaded image path (developer requested)
# We include the local path here so other scripts can transform to URL.
# ---------------------------------------------------------------------------
UPLOADED_IMAGE_URL = "file:///mnt/data/83fe929e-eaeb-4bce-b4fc-e399666e2daf.png"


# ---------------------------------------------------------------------------
# init_db: Create database, create ORM tables, and seed master data
# ---------------------------------------------------------------------------

def seed_operators(cursor):
    """
    Ensure operators table exists and seed operator rows from users where role='operator'.
    operator_id := users.emp_id
    operator_name := users.name

    Uses the provided pymysql cursor (DictCursor).
    """
    try:
        # Create table if missing (DDL). FK references users.emp_id (users.emp_id must be unique/PK).
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `operators` (
                `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `operator_id` INT NOT NULL,
                `operator_name` VARCHAR(255) NOT NULL,
                `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX (`operator_id`),
                CONSTRAINT `fk_operator_user` FOREIGN KEY (`operator_id`)
                    REFERENCES `users` (`emp_id`)
                    ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
    except Exception:
        # If FK fails due to users.emp_id not existing/unique, we still continue and try seeding without FK.
        logger.warning("Warning: Could not CREATE operators table with FK. Attempting create without FK.")
        logger.debug(traceback.format_exc())
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `operators` (
                    `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `operator_id` INT NOT NULL,
                    `operator_name` VARCHAR(255) NOT NULL,
                    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX (`operator_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
        except Exception:
            logger.warning("Warning: Could not create operators table (even without FK).")
            logger.debug(traceback.format_exc())
            return

    # Log the uploaded image path (developer requested to include local path)
    try:
        logger.info("Uploaded sample image (local path): %s", UPLOADED_IMAGE_URL)
    except Exception:
        pass

    # Fetch users with role = 'operator' and insert if missing
    try:
        cursor.execute("SELECT emp_id, name FROM users WHERE role = 'operator'")
        rows = cursor.fetchall() or []
    except Exception:
        logger.warning("Warning: Could not query users for role='operator' (table may not exist).")
        logger.debug(traceback.format_exc())
        return

    inserted = 0
    for r in rows:
        # r is a mapping because DictCursor
        emp_id = r.get("emp_id")
        name = r.get("name") or ""

        if emp_id is None:
            continue

        try:
            cursor.execute("SELECT 1 FROM operators WHERE operator_id = %s LIMIT 1", (emp_id,))
            exists = cursor.fetchone()
        except Exception:
            # If operators table missing for some reason, skip
            logger.debug("Could not check operators existence for %s", emp_id, exc_info=True)
            exists = None

        if exists:
            continue

        try:
            cursor.execute("INSERT INTO operators (operator_id, operator_name, created_at, updated_at) VALUES (%s, %s, NOW(), NOW())", (emp_id, name))
            inserted += 1
        except Exception:
            logger.warning("Failed to insert operator %s (%s).", emp_id, name)
            logger.debug(traceback.format_exc())

    try:
        logger.info("Operators seeding complete. Inserted %d new rows.", inserted)
    except Exception:
        pass
def seed_image_types(cursor):
    """
    Create image_type table (if missing) and insert default rows with 'count'.
    """
    # 1. Create Table with count column included
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS image_type (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                image_type VARCHAR(100) NOT NULL,
                description TEXT NULL,
                count INT NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
    except Exception:
        logger.warning("Warning: Could not CREATE image_type table.")

    # 2. Schema Migration: Add count column if it's missing (for existing databases)
    try:
        cursor.execute(
            "SELECT COUNT(*) as cnt FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='image_type' AND column_name='count'",
            (DB_NAME,)
        )
        if cursor.fetchone()['cnt'] == 0:
            logger.info("Migrating image_type: Adding 'count' column...")
            cursor.execute("ALTER TABLE image_type ADD COLUMN count INT NOT NULL DEFAULT 1")
            # Set Underside View (ID 4) to 2 explicitly after migration
            cursor.execute("UPDATE image_type SET count = 2 WHERE id = 4")
    except Exception:
        pass

    # 3. Seeding Data (Only runs if table is empty)
    try:
        cursor.execute("SELECT COUNT(*) AS cnt FROM image_type")
        cnt = cursor.fetchone().get("cnt", 0)
    except Exception:
        cnt = 0

    if cnt == 0:
        try:
            # Format: (Name, Description, Count)
            image_types = [
                ("Front View", "General tank photos", 1),
                ("Rear View", "Photos from rear side", 1),
                ("Top View", "Photos from top", 1),
                ("Underside View", "Photos of underside", 2), # <--- Count set to 2
                ("Front LH View", "Left-hand front view", 1),
                ("Rear LH View", "Left-hand rear view", 1),
                ("Front RH View", "Right-hand front view", 1),
                ("Rear RH View", "Right-hand rear view", 1),
                ("LH Side View", "Left side view", 1),
                ("RH Side View", "Right side view", 1),
                ("Valves Section View", "Valves section photos", 1),
                ("Safety Valve", "Safety valve photos", 1),
                ("Level / Pressure Gauge", "Photos showing gauge readings", 1),
                ("Vacuum Reading", "Vacuum reading photos", 1),
            ]
            for name, desc, count in image_types:
                cursor.execute(
                    "INSERT INTO image_type (image_type, description, count) VALUES (%s, %s, %s)",
                    (name, desc, count)
                )
            logger.info("Inserted default rows into image_type.")
        except Exception:
            logger.warning("Failed to insert default image_type rows.")
            logger.debug(traceback.format_exc())

def init_db():
    # 1) Create database if missing (connect without database)
    try:
        conn = get_db_connection(use_db=False)
    except Exception:
        logger.error("ERROR: Could not connect to MySQL server to create database.")
        logger.error(traceback.format_exc())
        return

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                )
                conn.commit()
                logger.info(f"Database `{DB_NAME}` ensured.")
            except Exception:
                logger.error(f"ERROR: Could not create database `{DB_NAME}`.")
                logger.error(traceback.format_exc())
                conn.rollback()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 1b) Execute tank_details.sql to create tank_details table and seed data
    try:
        sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "tank_details.sql")
        if os.path.exists(sql_file_path):
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
            
            conn = get_db_connection(use_db=True)
            try:
                with conn.cursor() as cur:
                    # Split and execute each statement
                    statements = sql_content.split(';')
                    for statement in statements:
                        stmt = statement.strip()
                        if stmt:  # Only execute non-empty statements
                            cur.execute(stmt)
                    conn.commit()
                    logger.info("tank_details.sql executed successfully.")
            except Exception as e:
                logger.warning(f"Warning: Could not execute tank_details.sql: {e}")
                logger.debug(traceback.format_exc())
                conn.rollback()
            finally:
                conn.close()
        else:
            logger.warning(f"Warning: tank_details.sql not found at {sql_file_path}")
    except Exception as e:
        logger.warning(f"Warning: Error executing tank_details.sql: {e}")
        logger.debug(traceback.format_exc())

    # 2) Import all SQLAlchemy models so Base.metadata knows the schema
    model_modules = [
        # keep these in sync with files inside app/models
        "inspection_status_model",
        "tank_status_model",
        "product_master_model",
        "inspection_type_model",
        "location_master_model",
        "operator_model",     
        "tank_header",
        "tank_details_model",
        "tank_certificate_model",
        "login_session_model",
        "safety_valve_brand_model",
        "safety_valve_model_model",
        "safety_valve_size_model",
        "inspection_job_model",
        "inspection_sub_job_model",
        "image_type_model",
        "inspection_checklist_model",
        "tank_images_model",
        "users_model",
        "tank_inspection_details",
        "to_do_list_model",
    ]

    for mod in model_modules:
        try:
            importlib.import_module(f"app.models.{mod}")
            logger.info(f"Imported model module: app.models.{mod}")
        except Exception as e:
            # keep going if one model import fails; print full traceback for diagnosis
            logger.warning(f"Warning: Could not import app.models.{mod}: {e}")
            logger.debug(traceback.format_exc())

    # 3) Create all tables via SQLAlchemy ORM
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Base.metadata.create_all() executed.")
    except Exception as e:
        logger.warning("Warning: Base.metadata.create_all failed:")
        logger.warning(traceback.format_exc())
        logger.info("Attempting manual table creation with deferred FK constraints...")
        
        # If create_all fails, manually create tables without FK constraints,
        # then add constraints after all tables are created
        try:
            with engine.begin() as conn:
                # Create users table first (no FK to other tables)
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `users` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `emp_id` INT NOT NULL UNIQUE,
                        `name` VARCHAR(100) NOT NULL,
                        `department` VARCHAR(100),
                        `designation` VARCHAR(100),
                        `hod` VARCHAR(100),
                        `supervisor` VARCHAR(100),
                        `email` VARCHAR(150) NOT NULL UNIQUE,
                        `password_hash` VARCHAR(255) NOT NULL,
                        `password_salt` VARCHAR(64) NOT NULL,
                        `role` VARCHAR(50),
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created users table.")
                
                # Create inspection_status table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `inspection_status` (
                        `status_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `status_name` VARCHAR(50) NOT NULL UNIQUE,
                        `description` VARCHAR(255)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created inspection_status table.")
                
                # Create tank_details table (parent table for FKs)
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `tank_details` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `tank_id` INT NOT NULL,
                        `tank_number` VARCHAR(50) NOT NULL,
                        `status` VARCHAR(50),
                        `mfgr` VARCHAR(100),
                        `date_mfg` DATE,
                        `pv_code` VARCHAR(50),
                        `un_iso_code` VARCHAR(50),
                        `capacity_l` INT,
                        `mawp` FLOAT,
                        `design_temperature` FLOAT,
                        `tare_weight_kg` INT,
                        `mgw_kg` INT,
                        `mpl_kg` INT,
                        `size` VARCHAR(100),
                        `pump_type` VARCHAR(50),
                        `vesmat` VARCHAR(50),
                        `gross_kg` INT,
                        `net_kg` INT,
                        `color_body_frame` VARCHAR(50),
                        `working_pressure` FLOAT,
                        `cabinet_type` VARCHAR(50),
                        `frame_type` VARCHAR(50),
                        `remark` VARCHAR(255),
                        `lease` VARCHAR(50),
                        `created_by` VARCHAR(50),
                        `updated_by` VARCHAR(50),
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY `uq_tank_details_tank_id` (`tank_id`),
                        UNIQUE KEY `uq_tank_details_tank_number` (`tank_number`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created tank_details table.")
                
                # Create indexes on tank_details for FK constraints
                try:
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tank_details_tank_id ON tank_details (tank_id)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tank_details_tank_number ON tank_details (tank_number)"))
                except Exception:
                    pass  # Indexes might already exist
                
                # Create tank_inspection_details
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `tank_inspection_details` (
                        `inspection_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `inspection_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        `report_number` VARCHAR(50) NOT NULL UNIQUE,
                        `tank_id` INT,
                        `tank_number` VARCHAR(50) NOT NULL,
                        `status_id` INT NOT NULL,
                        `product_id` INT NOT NULL,
                        `inspection_type_id` INT,
                        `location_id` INT,
                        `working_pressure` NUMERIC(12, 2),
                        `design_temperature` NUMERIC(12, 2),
                        `frame_type` VARCHAR(255),
                        `cabinet_type` VARCHAR(255),
                        `mfgr` VARCHAR(255),
                        `safety_valve_brand_id` INT,
                        `safety_valve_model_id` INT,
                        `safety_valve_size_id` INT,
                        `pi_next_inspection_date` DATE,
                        `notes` TEXT,
                        `lifter_weight` VARCHAR(255),
                        `emp_id` INT,
                        `operator_id` INT,
                        `ownership` VARCHAR(16),
                        `created_by` VARCHAR(100),
                        `updated_by` VARCHAR(100),
                        KEY `idx_tank_number` (`tank_number`),
                        KEY `idx_report_number` (`report_number`),
                        KEY `idx_inspection_date` (`inspection_date`),
                        KEY `idx_emp_id` (`emp_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                
                # Create inspection_checklist
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `inspection_checklist` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `inspection_id` INT NOT NULL,
                        `tank_id` INT,
                        `emp_id` INT,
                        `job_id` INT,
                        `job_name` VARCHAR(255),
                        `sub_job_id` INT,
                        `sn` VARCHAR(16) NOT NULL,
                        `sub_job_description` VARCHAR(512),
                        `status_id` INT NOT NULL DEFAULT 1,
                        `status_id` INT,
                        `comment` TEXT,
                        `flagged` BOOLEAN NOT NULL DEFAULT FALSE,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY `idx_inspection_id` (`inspection_id`),
                        UNIQUE KEY `uq_to_do_list_checklist_id` (`checklist_id`),
                        KEY `idx_tank_id` (`tank_id`),
                        KEY `idx_emp_id` (`emp_id`),
                        CONSTRAINT `fk_checklist_inspection` FOREIGN KEY (`inspection_id`)
                            REFERENCES `tank_inspection_details` (`inspection_id`) ON DELETE CASCADE,
                        CONSTRAINT `fk_checklist_tank` FOREIGN KEY (`tank_id`)
                            REFERENCES `tank_details` (`tank_id`) ON DELETE SET NULL,
                        CONSTRAINT `fk_checklist_user` FOREIGN KEY (`emp_id`)
                            REFERENCES `users` (`emp_id`) ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created inspection_checklist table.")
                
                # Create to_do_list table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `to_do_list` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `checklist_id` INT,
                        `inspection_id` INT NOT NULL,
                            `tank_id` INT,
                        `job_name` VARCHAR(255),
                        `sub_job_description` VARCHAR(512),
                        `sn` VARCHAR(16),
                        `status_id` INT,
                        `comment` TEXT,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            KEY `idx_inspection_id` (`inspection_id`),
                            KEY `idx_tank_id` (`tank_id`),
                            CONSTRAINT `fk_todo_tank` FOREIGN KEY (`tank_id`) REFERENCES `tank_details` (`tank_id`) ON DELETE SET NULL,
                        CONSTRAINT `fk_todo_inspection` FOREIGN KEY (`inspection_id`)
                            REFERENCES `tank_inspection_details` (`inspection_id`) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created to_do_list table.")
                
                # Create tank_certificate table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `tank_certificate` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `tank_id` INT NOT NULL,
                        `tank_number` VARCHAR(50) NOT NULL,
                        `year_of_manufacturing` VARCHAR(10),
                        `insp_2_5y_date` DATE,
                        `next_insp_date` DATE,
                        `certificate_number` VARCHAR(255) NOT NULL UNIQUE,
                        `certificate_file` VARCHAR(255),
                        `created_by` VARCHAR(100),
                        `updated_by` VARCHAR(100),
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        CONSTRAINT `fk_tank_certificate_tank_id` FOREIGN KEY (`tank_id`)
                            REFERENCES `tank_details` (`tank_id`) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created tank_certificate table.")
                
                # Create tank_images table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS `tank_images` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `emp_id` INT,
                        `inspection_id` INT,
                        `image_id` INT,
                        `image_type` VARCHAR(50) NOT NULL,
                        `tank_number` VARCHAR(50) NOT NULL,
                        `image_path` VARCHAR(255) NOT NULL,
                        `thumbnail_path` VARCHAR(255),
                        `created_date` DATE,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY `idx_inspection_id` (`inspection_id`),
                        UNIQUE KEY `uq_to_do_list_checklist_id` (`checklist_id`),
                        KEY `idx_image_id` (`image_id`),
                        KEY `idx_tank_number` (`tank_number`),
                        CONSTRAINT `fk_tank_images_inspection` FOREIGN KEY (`inspection_id`)
                            REFERENCES `tank_inspection_details` (`inspection_id`) ON DELETE SET NULL,
                        CONSTRAINT `fk_tank_images_image_type` FOREIGN KEY (`image_id`)
                            REFERENCES `image_type` (`id`) ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """))
                logger.info("Created tank_images table.")
                
        except Exception as fallback_err:
            logger.error("Failed to create tables manually:")
            logger.error(traceback.format_exc())

    # 3b) Inspect engine for tables; fallback for tank_header
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        logger.info(f"Existing tables after create_all: {tables}")
        if "tank_header" not in tables:
            logger.warning("`tank_header` table not present after create_all(). Attempting fallback CREATE TABLE.")
            fallback_sql = """
            CREATE TABLE IF NOT EXISTS `tank_header` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `tank_id` INT NULL,
                `tank_number` VARCHAR(50) NOT NULL,
                `status` VARCHAR(50) NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                `created_by` VARCHAR(100) NULL,
                `updated_by` VARCHAR(100) NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            with engine.begin() as conn:
                conn.execute(text(fallback_sql))
            logger.info("Fallback CREATE TABLE executed for tank_header.")
        else:
            logger.info("tank_header found; no fallback creation needed.")
    except Exception:
        logger.error("Error while inspecting or creating fallback for tank_header:")
        logger.error(traceback.format_exc())

    # 4) Now open a pymysql connection to the target database and run seeding.
    try:
        conn2 = get_db_connection(use_db=True)
    except Exception:
        logger.error("ERROR: Could not connect to the database for seeding. Check DB credentials / network.")
        logger.error(traceback.format_exc())
        return

    try:
        with conn2.cursor() as cursor:
            def safe_select_and_print(table_name):
                try:
                    cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 1")
                except Exception:
                    logger.debug(f"Could not SELECT from {table_name} (maybe table missing).")

            # ---------- SEED: tank_status ----------
            try:
                cursor.execute("CREATE TABLE IF NOT EXISTS tank_status (id INT AUTO_INCREMENT PRIMARY KEY, status_name VARCHAR(100) NOT NULL, description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;")
                cursor.execute("SELECT COUNT(*) AS cnt FROM tank_status")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    tank_status_data = [
                        ('Laden', 'Tank is loaded / filled'),
                        ('Empty', 'Tank is empty'),
                        ('Residue', 'Only residue remains'),
                    ]
                    for status_name, description in tank_status_data:
                        cursor.execute("INSERT INTO tank_status (status_name, description) VALUES (%s, %s)", (status_name, description))
                conn2.commit()
                safe_select_and_print("tank_status")
            except Exception:
                logger.warning("Warning: Could not seed tank_status")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: product_master ----------
            try:
                cursor.execute("""CREATE TABLE IF NOT EXISTS product_master (id INT AUTO_INCREMENT PRIMARY KEY, product_name VARCHAR(150) NOT NULL, description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;""")
                cursor.execute("SELECT COUNT(*) AS cnt FROM product_master")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    product_data = [
                        ('Liquid Argon', 'Cryogenic product - Liquid Argon'),
                        ('Liquid Carbon Dioxide', 'Cryogenic product - Liquid CO2'),
                        ('Liquid Oxygen', 'Cryogenic product - Liquid O2'),
                        ('Liquid Nitrogen', 'Cryogenic product - Liquid N2'),
                        ('Others', 'Other product - specified in notes'),
                    ]
                    for product_name, description in product_data:
                        cursor.execute("INSERT INTO product_master (product_name, description) VALUES (%s, %s)", (product_name, description))
                conn2.commit()
                safe_select_and_print("product_master")
            except Exception:
                logger.warning("Warning: Could not seed product_master")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: inspection_type ----------
            try:
                cursor.execute("""CREATE TABLE IF NOT EXISTS inspection_type (id INT AUTO_INCREMENT PRIMARY KEY, inspection_type_name VARCHAR(150) NOT NULL, description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;""")
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_type")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    inspection_type_data = [
                        ('Incoming', 'Incoming inspection'),
                        ('Outgoing', 'Outgoing inspection'),
                        ('On-Hire', 'On-hire inspection'),
                        ('Off-Hire', 'Off-hire inspection'),
                        ('Condition', 'Condition check'),
                    ]
                    for it_name, description in inspection_type_data:
                        cursor.execute("INSERT INTO inspection_type (inspection_type_name, description) VALUES (%s, %s)", (it_name, description))
                conn2.commit()
                safe_select_and_print("inspection_type")
            except Exception:
                logger.warning("Warning: Could not seed inspection_type")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: location_master ----------
            try:
                cursor.execute("""CREATE TABLE IF NOT EXISTS location_master (id INT AUTO_INCREMENT PRIMARY KEY, location_name VARCHAR(150) NOT NULL, description TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;""")
                cursor.execute("SELECT COUNT(*) AS cnt FROM location_master")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    location_data = [
                        ('SG-1 16A, Benoi Cresent', 'Default location'),
                        ('SG-2 5A Jalan Papan', 'Alternate location'),
                        ('China QD', 'China QD location'),
                    ]
                    for loc_name, description in location_data:
                        cursor.execute("INSERT INTO location_master (location_name, description) VALUES (%s, %s)", (loc_name, description))
                conn2.commit()
                safe_select_and_print("location_master")
            except Exception:
                logger.warning("Warning: Could not seed location_master")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ... (the rest of your seeding for inspection_status, inspection_job etc. kept as-is)
            # ---------- SEED: inspection_status ----------
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_status")
                cnt = cursor.fetchone().get('cnt', 0)
            except Exception:
                cnt = 0
            if cnt == 0:
                try:
                    status_rows = [
                        ('OK', 'Inspection passed'),
                        ('Faulty', 'Requires attention or repair'),
                        ('Not Inspected', 'Not yet inspected')
                    ]
                    for name, desc in status_rows:
                        cursor.execute("INSERT INTO inspection_status (status_name, description) VALUES (%s, %s)", (name, desc))
                    conn2.commit()
                    logger.info('Seeded inspection_status rows')
                except Exception:
                    logger.warning('Could not seed inspection_status')
                    logger.debug(traceback.format_exc())
                    conn2.rollback()

            # ---------- SEED: inspection_job ----------
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_job")
                cnt = cursor.fetchone().get('cnt', 0)
            except Exception:
                cnt = 0
            if cnt == 0:
                try:
                    jobs = [
                        ('Tank Body & Frame Condition', ''),
                        ('Pipework & Installation', ''),
                        ('Tank Instrument & Assembly', ''),
                        ('Valves Tightness & Operation', ''),
                        ('Before Departure Check', ''),
                        ('Others Observation & Comment', ''),
                    ]
                    for name, desc in jobs:
                        # database.table uses job_name/description columns
                        cursor.execute("INSERT INTO inspection_job (job_name, description, created_at, updated_at) VALUES (%s, %s, NOW(), NOW())", (name, desc))
                    conn2.commit()
                    logger.info('Seeded inspection_job rows')
                except Exception:
                    logger.warning('Could not seed inspection_job')
                    logger.debug(traceback.format_exc())
                    conn2.rollback()

            # ---------- SEED: inspection_sub_job ----------
            try:
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_sub_job")
                cnt = cursor.fetchone().get('cnt', 0)
            except Exception:
                cnt = 0
            if cnt == 0:
                try:
                    # Minimal sub-job items per job; these are illustrative and map to the sample checklist
                    sub_jobs = {
                        1: [
                            ('Body x 6 Sides & All Frame – No Dent / No Bent / No Deep Cut', ''),
                            ('Cabin Door & Frame Condition – No Damage / Can Lock', ''),
                            ('Tank Number, Product & Hazchem Label – Not Missing or Tear', ''),
                            ('Condition of Paint Work & Cleanliness – Clean / No Bad Rust', ''),
                            ('Others', ''),
                        ],
                        2: [
                            ('Pipework Supports / Brackets – Not Loose / No Bent', ''),
                            ('Pipework Joint & Welding – No Crack / No Icing / No Leaking', ''),
                            ('Earthing Point', ''),
                            ('PBU Support & Flange Connection – No Leak / Not Damage', ''),
                            ('Others', ''),
                        ],
                        3: [
                            ('Safety Diverter Valve – Switching Lever', ''),
                            ('Safety Valves Connection & Joint – No Leaks', ''),
                            ('Level & Pressure Gauge Support Bracket, Connection & Joint – Not Loosen / No Leaks', ''),
                            ('Level & Pressure Gauge – Function Check', ''),
                            ('Level & Pressure Gauge Valve Open / Balance Valve Close', ''),
                            ('Data & CSC Plate – Not Missing / Not Damage', ''),
                            ('Others', ''),
                        ],
                        4: [
                            ('Valve Handwheel – Not Missing / Nut Not Loose', ''),
                            ('Valve Open & Close Operation – No Seizing / Not Tight / Not Jam', ''),
                            ('Valve Tightness Incl Glands – No Leak / No Icing / No Passing', ''),
                            ('Anchor Point', ''),
                            ('Others', ''),
                        ],
                        5: [
                            ('All Valves Closed – Defrost & Close Firmly', ''),
                            ('Caps fitted to Outlets or Cover from Dust if applicable', ''),
                            ('Security Seal Fitted by Refilling Plant - Check', ''),
                            ('Pressure Gauge – lowest possible', ''),
                            ('Level Gauge – Within marking or standard indication', ''),
                            ('Weight Reading – ensure within acceptance weight', ''),
                            ('Cabin Door Lock – Secure and prevent from sudden opening', ''),
                            ('Others', ''),
                        ],
                        6: [
                            ('Other observations / general comments', ''),
                        ]
                    }
                    for jid, items in sub_jobs.items():
                        for name, desc in items:
                            cursor.execute("INSERT INTO inspection_sub_job (job_id, sub_job_name, description, created_at, updated_at) VALUES (%s, %s, %s, NOW(), NOW())", (jid, name, desc))
                    conn2.commit()
                    logger.info('Seeded inspection_sub_job rows')
                except Exception:
                    logger.warning('Could not seed inspection_sub_job')
                    logger.debug(traceback.format_exc())
                    conn2.rollback()
            # Ensure `sub_job_id` and `sn` columns exist on inspection_sub_job, and populate them
            try:
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM information_schema.columns WHERE table_schema=%s AND table_name='inspection_sub_job' AND column_name='sub_job_id'",
                    (DB_NAME,)
                )
                cnt_sub_id = cursor.fetchone().get('cnt', 0)
            except Exception:
                cnt_sub_id = 0

            if cnt_sub_id == 0:
                try:
                    logger.info("Migrating inspection_sub_job: Adding 'sub_job_id' column...")
                    cursor.execute("ALTER TABLE inspection_sub_job ADD COLUMN sub_job_id INT NULL")
                    conn2.commit()
                except Exception:
                    conn2.rollback()

            try:
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM information_schema.columns WHERE table_schema=%s AND table_name='inspection_sub_job' AND column_name='sn'",
                    (DB_NAME,)
                )
                cnt_sn = cursor.fetchone().get('cnt', 0)
            except Exception:
                cnt_sn = 0

            if cnt_sn == 0:
                try:
                    logger.info("Migrating inspection_sub_job: Adding 'sn' column...")
                    cursor.execute("ALTER TABLE inspection_sub_job ADD COLUMN sn VARCHAR(32) NULL")
                    conn2.commit()
                except Exception:
                    conn2.rollback()

            # Populate sub_job_id and sn where NULL: assign position per job (1-based) and sn as '<job_id>.<pos>'
            try:
                cursor.execute("SELECT DISTINCT job_id FROM inspection_sub_job")
                jobs_in_table = [r.get('job_id') for r in cursor.fetchall() or []]
                for jid in jobs_in_table:
                    try:
                        cursor.execute("SELECT id FROM inspection_sub_job WHERE job_id=%s ORDER BY id", (jid,))
                        rows = cursor.fetchall() or []
                        pos = 1
                        for r in rows:
                            rid = r.get('id')
                            # Only update if sub_job_id or sn is NULL
                            try:
                                cursor.execute("SELECT sub_job_id, sn FROM inspection_sub_job WHERE id=%s LIMIT 1", (rid,))
                                curvals = cursor.fetchone() or {}
                                need_update = False
                                updates = {}
                                if curvals.get('sub_job_id') is None:
                                    updates['sub_job_id'] = pos
                                    need_update = True
                                if not curvals.get('sn'):
                                    updates['sn'] = f"{jid}.{pos}"
                                    need_update = True
                                if need_update:
                                    cursor.execute(
                                        "UPDATE inspection_sub_job SET sub_job_id=%s, sn=%s, updated_at=NOW() WHERE id=%s",
                                        (updates.get('sub_job_id'), updates.get('sn'), rid)
                                    )
                                    conn2.commit()
                            except Exception:
                                conn2.rollback()
                            pos += 1
                    except Exception:
                        logger.debug(traceback.format_exc())
            except Exception:
                logger.debug(traceback.format_exc())
            # For brevity here I kept the same operations as you provided. They will run exactly as before.

            # Ensure lifter_weight & safety_valve_* columns exist on tank_inspection_details (safe checks)
            try:
                cursor.execute(
                    "SELECT COUNT(*) as cnt FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name='tank_inspection_details' "
                    "AND column_name='lifter_weight'",
                    (DB_NAME,)
                )
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    try:
                        cursor.execute("ALTER TABLE `tank_inspection_details` ADD COLUMN lifter_weight VARCHAR(255) NULL")
                        conn2.commit()
                    except Exception:
                        conn2.rollback()
            except Exception:
                logger.debug("Could not ensure lifter_weight column (table may not exist).")
                conn2.rollback()

            for col_def in [
                ("safety_valve_brand_id", "INT NULL"),
                ("safety_valve_model_id", "INT NULL"),
                ("safety_valve_size_id", "INT NULL"),
            ]:
                colname, coltype = col_def
                try:
                    cursor.execute(
                        "SELECT COUNT(*) AS cnt FROM information_schema.columns WHERE table_schema=%s AND table_name='tank_inspection_details' AND column_name=%s",
                        (DB_NAME, colname)
                    )
                    cnt = cursor.fetchone().get('cnt', 0)
                    if cnt == 0:
                        try:
                            cursor.execute(f"ALTER TABLE `tank_inspection_details` ADD COLUMN `{colname}` {coltype}")
                            conn2.commit()
                        except Exception:
                            conn2.rollback()
                except Exception:
                    conn2.rollback()

            # ---------- SEED: OPERATORS ----------
            try:
                seed_operators(cursor)
                conn2.commit()
                logger.info("Operators table seeded.")
            except Exception:
                logger.warning("Warning: Could not seed operators")
                logger.debug(traceback.format_exc())
                conn2.rollback()
            
                        # ---------- SEED: image_type ----------
            try:
                seed_image_types(cursor)
                conn2.commit()
                safe_select_and_print("image_type")
            except Exception:
                logger.warning("Warning: Could not seed image_type")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: inspection_checklist ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `inspection_checklist` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `inspection_id` INT NOT NULL,
                        `tank_id` INT,
                        `emp_id` INT,
                        `job_id` INT,
                        `job_name` VARCHAR(255),
                        `sub_job_id` INT,
                        `sn` VARCHAR(16) NOT NULL,
                        `sub_job_description` VARCHAR(512),
                        `status_id` INT NOT NULL DEFAULT 1,
                        `status` VARCHAR(32),
                        `comment` TEXT,
                        `flagged` BOOLEAN NOT NULL DEFAULT FALSE,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY `idx_inspection_id` (`inspection_id`),
                        KEY `idx_tank_id` (`tank_id`),
                        KEY `idx_emp_id` (`emp_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created inspection_checklist table.")
            except Exception:
                logger.warning("Warning: Could not create inspection_checklist table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: to_do_list ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `to_do_list` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `checklist_id` INT,
                        `inspection_id` INT NOT NULL,
                            `tank_id` INT,
                        `job_name` VARCHAR(255),
                        `sub_job_description` VARCHAR(512),
                        `sn` VARCHAR(16),
                        `status` VARCHAR(32),
                        `comment` TEXT,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            KEY `idx_inspection_id` (`inspection_id`),
                            KEY `idx_tank_id` (`tank_id`),
                            CONSTRAINT `fk_todo_tank` FOREIGN KEY (`tank_id`) REFERENCES `tank_details` (`tank_id`) ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created to_do_list table.")
            except Exception:
                logger.warning("Warning: Could not create to_do_list table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: tank_images ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `tank_images` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `emp_id` INT,
                        `inspection_id` INT,
                        `image_id` INT,
                        `image_type` VARCHAR(50) NOT NULL,
                        `tank_number` VARCHAR(50) NOT NULL,
                        `image_path` VARCHAR(255) NOT NULL,
                        `thumbnail_path` VARCHAR(255),
                        `created_date` DATE,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY `idx_inspection_id` (`inspection_id`),
                        KEY `idx_image_id` (`image_id`),
                        KEY `idx_tank_number` (`tank_number`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created tank_images table.")
            except Exception:
                logger.warning("Warning: Could not create tank_images table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: tank_certificate ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `tank_certificate` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `tank_id` INT NOT NULL,
                        `tank_number` VARCHAR(50) NOT NULL,
                        `year_of_manufacturing` VARCHAR(10),
                        `insp_2_5y_date` DATE,
                        `next_insp_date` DATE,
                        `certificate_number` VARCHAR(255) NOT NULL UNIQUE,
                        `certificate_file` VARCHAR(255),
                        `created_by` VARCHAR(100),
                        `updated_by` VARCHAR(100),
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created tank_certificate table.")
            except Exception:
                logger.warning("Warning: Could not create tank_certificate table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: login_session ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `login_session` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `emp_id` INT NOT NULL,
                        `token` VARCHAR(500),
                        `still_logged_in` INT DEFAULT 1,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created login_session table.")
            except Exception:
                logger.warning("Warning: Could not create login_session table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: inspection_job ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `inspection_job` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `job_name` VARCHAR(255) NOT NULL,
                        `description` TEXT,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created inspection_job table.")
            except Exception:
                logger.warning("Warning: Could not create inspection_job table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: inspection_sub_job (internal id + per-job sub_job_id + sn) ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `inspection_sub_job` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `job_id` INT NOT NULL,
                        `sub_job_id` INT NULL,
                        `sn` VARCHAR(32) NULL,
                        `sub_job_name` VARCHAR(255) NOT NULL,
                        `sub_job_description` VARCHAR(512) NULL,
                        `description` TEXT,
                        `sort_order` INT DEFAULT 0,
                        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        KEY `idx_ins_sub_job_job_id` (`job_id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created inspection_sub_job table.")
            except Exception:
                logger.warning("Warning: Could not create inspection_sub_job table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: safety_valve_brand ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `safety_valve_brand` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `brand_name` VARCHAR(100) NOT NULL,
                        `description` TEXT,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created safety_valve_brand table.")
            except Exception:
                logger.warning("Warning: Could not create safety_valve_brand table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: safety_valve_model ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `safety_valve_model` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `model_name` VARCHAR(100) NOT NULL,
                        `description` TEXT,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created safety_valve_model table.")
            except Exception:
                logger.warning("Warning: Could not create safety_valve_model table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

            # ---------- CREATE: safety_valve_size ----------
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `safety_valve_size` (
                        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        `size_label` VARCHAR(100) NOT NULL,
                        `description` TEXT,
                        `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                        `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """)
                conn2.commit()
                logger.info("Created safety_valve_size table.")
                # --- Seed some default values for safety valve masters if empty ---
                # Use cursor/conn2 for seeding to ensure changes are committed via conn2
                try:
                    cursor.execute("SELECT COUNT(1) as cnt FROM safety_valve_brand")
                    r = cursor.fetchone() or {"cnt": 0}
                    cnt = int(r.get("cnt", 0) if isinstance(r, dict) else (r[0] if r else 0))
                    if cnt == 0:
                        safety_valve_brand_data = [
                            ('Generic Brand', 'Generic safety valves'),
                            ('Fisher', 'Fisher safety valves'),
                            ('TESCOM', 'TESCOM safety valves'),
                            ('Bonomi', 'Bonomi safety valves'),
                            ('Other', 'Other brands')
                        ]
                        for brand_name, description in safety_valve_brand_data:
                            cursor.execute("INSERT INTO safety_valve_brand (brand_name, description) VALUES (%s, %s)", (brand_name, description))
                        conn2.commit()
                except Exception:
                    try:
                        conn2.rollback()
                    except Exception:
                        pass
                # NOTE: We intentionally DO NOT seed any rows into safety_valve_model to allow
                # the table to be empty on startup per user request. If the table is populated
                # from a previous run, we will clear it here so that it becomes empty.
                try:
                    cursor.execute("DELETE FROM safety_valve_model")
                    conn2.commit()
                    logger.info("Cleared safety_valve_model table — it is now empty.")
                except Exception:
                    try:
                        conn2.rollback()
                    except Exception:
                        pass
                # NOTE: We intentionally DO NOT seed any rows into safety_valve_size to allow
                # the table to be empty on startup per user request. If the table is populated
                # from a previous run, we will clear it here so that it becomes empty.
                try:
                    cursor.execute("DELETE FROM safety_valve_size")
                    conn2.commit()
                    logger.info("Cleared safety_valve_size table — it is now empty.")
                except Exception:
                    try:
                        conn2.rollback()
                    except Exception:
                        pass
            except Exception:
                logger.warning("Warning: Could not create safety_valve_size table")
                logger.debug(traceback.format_exc())
                conn2.rollback()

    finally:
        try:
            conn2.close()
        except Exception:
            pass


# Call init_db() on module import (keep if desired)
init_db()
