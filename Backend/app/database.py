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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from typing import Generator

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
# init_db: Create database, create ORM tables, and seed master data
# ---------------------------------------------------------------------------

def init_db():
    # 1) Create database if missing (connect without database)
    try:
        conn = get_db_connection(use_db=False)
    except Exception as e:
        print("ERROR: Could not connect to MySQL server to create database.")
        print(traceback.format_exc())
        return

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                )
                conn.commit()
            except Exception as e:
                print(f"ERROR: Could not create database `{DB_NAME}`: {e}")
                print(traceback.format_exc())
                conn.rollback()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 2) Import all SQLAlchemy models so Base.metadata knows the schema
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
            print(traceback.format_exc())

    # 3) Create all tables via SQLAlchemy ORM
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print("Warning: Base.metadata.create_all failed:")
        print(traceback.format_exc())

    # 4) Now open a pymysql connection to the target database and run seeding.
    try:
        conn2 = get_db_connection(use_db=True)
    except Exception as e:
        print("ERROR: Could not connect to the database for seeding. Check DB credentials / network.")
        print(traceback.format_exc())
        return

    try:
        with conn2.cursor() as cursor:
            # Helper to run checked inserts (and print inserted rows after commit)
            def safe_select_and_print(table_name):
                try:
                    cursor.execute(f"SELECT * FROM `{table_name}`")
                    rows = cursor.fetchall()
                    # Intentionally no printing here except warnings/errors
                except Exception as e:
                    print(f"Warning: Could not SELECT * from {table_name}: {e}")
                    print(traceback.format_exc())

            # ---------- SEED: tank_status ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'tank_status'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tank_status (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            status_name VARCHAR(100) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM tank_status")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
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
                conn2.commit()
                safe_select_and_print("tank_status")
            except Exception as e:
                print(f"Warning: Could not seed tank_status: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: product_master ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'product_master'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS product_master (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            product_name VARCHAR(150) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
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
                        cursor.execute(
                            "INSERT INTO product_master (product_name, description) VALUES (%s, %s)",
                            (product_name, description)
                        )
                conn2.commit()
                safe_select_and_print("product_master")
            except Exception as e:
                print(f"Warning: Could not seed product_master: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: inspection_type ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'inspection_type'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS inspection_type (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            inspection_type_name VARCHAR(150) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
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
                        cursor.execute(
                            "INSERT INTO inspection_type (inspection_type_name, description) VALUES (%s, %s)",
                            (it_name, description)
                        )
                conn2.commit()
                safe_select_and_print("inspection_type")
            except Exception as e:
                print(f"Warning: Could not seed inspection_type: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: location_master ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'location_master'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS location_master (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            location_name VARCHAR(150) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM location_master")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
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
                conn2.commit()
                safe_select_and_print("location_master")
            except Exception as e:
                print(f"Warning: Could not seed location_master: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: inspection_status ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'inspection_status'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS inspection_status (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            status_name VARCHAR(100) NOT NULL,
                            description TEXT,
                            sort_order INT DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_status")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    status_data = [
                        ('OK', 'All checks passed', 1),
                        ('FAULTY', 'Issues found that need attention', 2),
                        ('NA', 'Not applicable for this tank', 3),
                    ]
                    for status_name, description, sort_order in status_data:
                        cursor.execute(
                            "INSERT INTO inspection_status (status_name, description, sort_order) "
                            "VALUES (%s, %s, %s)",
                            (status_name, description, sort_order)
                        )
                conn2.commit()
                safe_select_and_print("inspection_status")
            except Exception as e:
                print(f"Warning: Could not seed inspection_status: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: inspection_job ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'inspection_job'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS inspection_job (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            job_code VARCHAR(50),
                            job_description VARCHAR(255),
                            sort_order INT DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_job")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
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
                            "INSERT INTO inspection_job (job_code, job_description, sort_order) "
                            "VALUES (%s, %s, %s)",
                            (job_code, job_desc, sort_order)
                        )
                conn2.commit()
                safe_select_and_print("inspection_job")
            except Exception as e:
                print(f"Warning: Could not seed inspection_job: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: inspection_sub_job ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'inspection_sub_job'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS inspection_sub_job (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            job_id INT NOT NULL,
                            sn VARCHAR(50),
                            sub_job_description TEXT,
                            sort_order INT DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (job_id) REFERENCES inspection_job(id) ON DELETE CASCADE
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM inspection_sub_job")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
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
                            "INSERT INTO inspection_sub_job (job_id, sn, sub_job_description, sort_order) "
                            "VALUES (%s, %s, %s, %s)",
                            (job_id, sn, sub_job_desc, sort_order)
                        )
                conn2.commit()
                safe_select_and_print("inspection_sub_job")
            except Exception as e:
                print(f"Warning: Could not seed inspection_sub_job: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: safety_valve_brand ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'safety_valve_brand'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS safety_valve_brand (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            brand_name VARCHAR(150) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM safety_valve_brand")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    brand_data = [
                        ('Crosby', 'Crosby safety valve brand'),
                        ('LESER', 'LESER safety valve brand'),
                        ('Farris', 'Farris safety relief valves'),
                        ('Bopp & Reuther', 'Bopp & Reuther safety valves'),
                        ('Consolidated', 'Consolidated safety valve brand'),
                    ]
                    for brand_name, description in brand_data:
                        cursor.execute(
                            "INSERT INTO safety_valve_brand (brand_name, description) VALUES (%s, %s)",
                            (brand_name, description)
                        )
                conn2.commit()
                safe_select_and_print("safety_valve_brand")
            except Exception as e:
                print(f"Warning: Could not seed safety_valve_brand: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: safety_valve_model ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'safety_valve_model'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS safety_valve_model (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            model_name VARCHAR(150) NOT NULL,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM safety_valve_model")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    model_data = [
                        ('SV-100', 'Standard safety valve model SV-100'),
                        ('SV-150', 'Standard safety valve model SV-150'),
                        ('CRYO-200', 'Cryogenic service safety valve CRYO-200'),
                        ('HI-PRESS-50', 'High pressure safety valve HI-PRESS-50'),
                        ('LOW-SET-10', 'Low set pressure safety valve LOW-SET-10'),
                    ]
                    for model_name, description in model_data:
                        cursor.execute(
                            "INSERT INTO safety_valve_model (model_name, description) VALUES (%s, %s)",
                            (model_name, description)
                        )
                conn2.commit()
                safe_select_and_print("safety_valve_model")
            except Exception as e:
                print(f"Warning: Could not seed safety_valve_model: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- SEED: safety_valve_size ----------
            try:
                cursor.execute("SHOW TABLES LIKE 'safety_valve_size'")
                if cursor.rowcount == 0:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS safety_valve_size (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            size_label VARCHAR(50) NOT NULL,
                            description VARCHAR(255),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB;
                    """)
                    conn2.commit()
                cursor.execute("SELECT COUNT(*) AS cnt FROM safety_valve_size")
                cnt = cursor.fetchone().get('cnt', 0)
                if cnt == 0:
                    sizes = ['1', '2.5', '3', '6', '2']
                    for val in sizes:
                        cursor.execute(
                            "INSERT INTO safety_valve_size (size_label, description) VALUES (%s, %s)",
                            (val, f"Size {val}")
                        )
                conn2.commit()
                safe_select_and_print("safety_valve_size")
            except Exception as e:
                print(f"Warning: Could not seed safety_valve_size: {e}")
                print(traceback.format_exc())
                conn2.rollback()

            # ---------- Ensure lifter_weight column exists on tank_inspection_details ----------
            try:
                try:
                    cursor.execute(
                        "SELECT COUNT(*) as cnt FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name='tank_inspection_details' "
                        "AND column_name='lifter_weight'",
                        (DB_NAME,)
                    )
                    cnt = cursor.fetchone().get('cnt', 0)
                    if cnt == 0:
                        cursor.execute(
                            "ALTER TABLE `tank_inspection_details` "
                            "ADD COLUMN lifter_weight VARCHAR(255) NULL"
                        )
                        conn2.commit()
                except Exception as e:
                    print("Warning: Could not ensure lifter_weight column via ALTER (table may not exist).")
                    print(traceback.format_exc())
                    conn2.rollback()
            except Exception:
                pass

            # ---------- Inspect/drop problematic unique index on tank_images ----------
            try:
                cursor.execute("""
                    SELECT INDEX_NAME,
                           GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols,
                           ANY_VALUE(NON_UNIQUE) AS NON_UNIQUE
                    FROM information_schema.statistics
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME='tank_images'
                    GROUP BY INDEX_NAME
                """, (DB_NAME,))
                indexes = cursor.fetchall()
                if not indexes:
                    # table may not exist; that's fine
                    pass
                for idx in indexes:
                    try:
                        cols = idx.get('cols') or ''
                        non_unique = idx.get('NON_UNIQUE')
                        name = idx.get('INDEX_NAME')
                        colset = set([cname.strip() for cname in cols.split(',')]) if cols else set()
                        target = {'tank_number', 'image_type', 'created_date'}
                        if non_unique == 0 and target.issubset(colset):
                            try:
                                cursor.execute(f"ALTER TABLE `tank_images` DROP INDEX `{name}`")
                                conn2.commit()
                            except Exception as drop_e:
                                print(f"Warning: Could not drop index {name}: {drop_e}")
                                print(traceback.format_exc())
                    except Exception as e:
                        print(f"Warning while inspecting index entry {idx}: {e}")
                        print(traceback.format_exc())
            except Exception as e:
                print(f"Warning: could not inspect/drop tank_images indexes: {e}")
                print(traceback.format_exc())

    finally:
        try:
            conn2.close()
        except Exception:
            pass


# Call init_db() on module import
init_db()
