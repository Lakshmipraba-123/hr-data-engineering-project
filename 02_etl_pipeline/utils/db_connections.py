# ============================================================
# FILE: 02_etl_pipeline/utils/db_connections.py
# PURPOSE: Central place for all database connection logic
#          All other ETL files import from here
# ============================================================

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# ── Load .env file ────────────────────────────────────────────
dotenv_path = Path("C:/hr-data-engineering-project/.env")
load_dotenv(dotenv_path=dotenv_path)

# ── Read all config from .env ─────────────────────────────────
DB_HOST          = os.getenv('DB_HOST',          'localhost')
DB_PORT          = os.getenv('DB_PORT',          '5432')
DB_NAME          = os.getenv('DB_NAME',          'hr_source_db')
DB_USER          = os.getenv('DB_USER',          'postgres')
DB_PASSWORD      = os.getenv('DB_PASSWORD',      'admin123')
SOURCE_SCHEMA    = os.getenv('SOURCE_SCHEMA',    'public')
WAREHOUSE_SCHEMA = os.getenv('WAREHOUSE_SCHEMA', 'hr_warehouse')


# ═══════════════════════════════════════════════════════════════
# FUNCTION 1 — get_source_engine()
# Returns a SQLAlchemy engine connected to the SOURCE schema
# ═══════════════════════════════════════════════════════════════
def get_source_engine():
    """
    Creates and returns a database engine for the SOURCE database.
    The source database contains our raw operational HR tables.

    Returns:
        SQLAlchemy Engine object connected to source DB
    """
    # Format: dialect+driver://user:password@host:port/database
    url = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}"
        f"/{DB_NAME}"
    )

    engine = create_engine(url, pool_pre_ping=True) # pool_pre_ping=True checks connection is alive before using it
    return engine


# ═══════════════════════════════════════════════════════════════
# FUNCTION 2 — get_warehouse_engine()
# Returns a SQLAlchemy engine connected to the WAREHOUSE schema
# ═══════════════════════════════════════════════════════════════
def get_warehouse_engine():
    """
    Creates and returns a database engine for the WAREHOUSE schema.
    The warehouse uses the same PostgreSQL database but a different
    schema — this is how we separate source and warehouse cleanly.

    Returns:
        SQLAlchemy Engine object connected to warehouse schema
    """
  
    url = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}"
        f"/{DB_NAME}"
    )

    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"options": f"-c search_path={WAREHOUSE_SCHEMA}"}
    )
    return engine


# ═══════════════════════════════════════════════════════════════
# FUNCTION 3 — create_warehouse_schema()
# Creates the hr_warehouse schema in PostgreSQL if it doesn't exist
# Also creates all Star Schema tables
# ═══════════════════════════════════════════════════════════════
def create_warehouse_schema():
    """
    Creates the warehouse schema and all dimension + fact tables.
    """

    engine = get_source_engine()

    sql = f"""
    -- Create the warehouse schema if it doesn't exist
    CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA};

    -- ─────────────────────────────────────────
    -- DIMENSION TABLE: dim_date
    -- ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.dim_date (
        date_key        INTEGER PRIMARY KEY,   -- Format: YYYYMMDD e.g. 20240315
        full_date       DATE NOT NULL,
        year            INTEGER,
        quarter         INTEGER,
        quarter_name    VARCHAR(10),           -- e.g. 'Q1 2024'
        month           INTEGER,
        month_name      VARCHAR(10),           -- e.g. 'January'
        week_of_year    INTEGER,
        day_of_week     INTEGER,               -- 1=Monday, 7=Sunday
        day_name        VARCHAR(10),           -- e.g. 'Monday'
        is_weekend      BOOLEAN,
        is_holiday      BOOLEAN DEFAULT FALSE
    );

    -- ─────────────────────────────────────────
    -- DIMENSION TABLE: dim_department
    -- ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.dim_department (
        department_key  SERIAL PRIMARY KEY,    -- Surrogate key (our warehouse ID)
        department_id   INTEGER NOT NULL,      -- Natural key (from source system)
        department_name VARCHAR(100),
        location        VARCHAR(100),
        manager_name    VARCHAR(100),
        effective_date  DATE NOT NULL,         -- SCD Type 2: when record became active
        expiry_date     DATE DEFAULT '9999-12-31', -- SCD Type 2: far future = still active
        is_current      BOOLEAN DEFAULT TRUE   -- Quick filter for latest record
    );

    -- ─────────────────────────────────────────
    -- DIMENSION TABLE: dim_employee
    -- SCD Type 2 — keeps full history of changes
    -- ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.dim_employee (
        employee_key    SERIAL PRIMARY KEY,    -- Surrogate key
        employee_id     INTEGER NOT NULL,      -- Natural key from source
        first_name      VARCHAR(50),
        last_name       VARCHAR(50),
        full_name       VARCHAR(100),          -- Pre-built: first + last
        email           VARCHAR(100),
        job_title       VARCHAR(100),
        department_name VARCHAR(100),          -- Denormalized: copied from dept
        hire_date       DATE,
        status          VARCHAR(20),
        tenure_years    DECIMAL(5,2),          -- Pre-calculated: saves analysts time
        effective_date  DATE NOT NULL,
        expiry_date     DATE DEFAULT '9999-12-31',
        is_current      BOOLEAN DEFAULT TRUE
    );

    -- ─────────────────────────────────────────
    -- DIMENSION TABLE: dim_project
    -- ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.dim_project (
        project_key     SERIAL PRIMARY KEY,
        project_id      INTEGER NOT NULL,
        project_name    VARCHAR(200),
        department_name VARCHAR(100),
        status          VARCHAR(20),
        start_date      DATE,
        end_date        DATE,
        budget          DECIMAL(15,2),
        duration_days   INTEGER                -- Pre-calculated: end - start
    );

    -- ─────────────────────────────────────────
    -- DIMENSION TABLE: dim_leave_type
    -- ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.dim_leave_type (
        leave_type_key  SERIAL PRIMARY KEY,
        leave_type      VARCHAR(50),
        category        VARCHAR(50),           -- 'personal', 'medical', 'parental'
        is_paid         BOOLEAN
    );

    -- ─────────────────────────────────────────
    -- FACT TABLE: fact_employee_snapshot
    -- One row per employee per month
    -- ─────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS {WAREHOUSE_SCHEMA}.fact_employee_snapshot (
        snapshot_id         SERIAL PRIMARY KEY,
        -- Foreign keys pointing to dimension tables
        employee_key        INTEGER REFERENCES {WAREHOUSE_SCHEMA}.dim_employee(employee_key),
        department_key      INTEGER REFERENCES {WAREHOUSE_SCHEMA}.dim_department(department_key),
        date_key            INTEGER REFERENCES {WAREHOUSE_SCHEMA}.dim_date(date_key),
        project_key         INTEGER REFERENCES {WAREHOUSE_SCHEMA}.dim_project(project_key),
        -- Measures — the actual numbers we analyze
        salary_amount       DECIMAL(12,2),     -- Current salary at snapshot time
        performance_rating  DECIMAL(3,1),      -- Latest rating (1.0 to 5.0)
        leave_days_taken    INTEGER DEFAULT 0, -- Total leave days in that month
        project_count       INTEGER DEFAULT 0, -- Active projects at snapshot time
        is_active           BOOLEAN,           -- Was employee active that month?
        -- Audit columns
        load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_system       VARCHAR(50) DEFAULT 'hr_postgresql'
    );
    """

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    print(f"✅ Warehouse schema '{WAREHOUSE_SCHEMA}' created successfully")
    print(f"   Tables created: dim_date, dim_department, dim_employee,")
    print(f"                   dim_project, dim_leave_type, fact_employee_snapshot")


# ═══════════════════════════════════════════════════════════════
# FUNCTION 4 — test_connections()
# Quick health check — run this to verify everything works
# ═══════════════════════════════════════════════════════════════
def test_connections():
    """
    Tests both source and warehouse connections.
    """
    print("\n🔍 Testing database connections...\n")

    # Test source connection
    try:
        source_engine = get_source_engine()
        with source_engine.connect() as conn:
            # Count employees to verify source data exists
            result = conn.execute(text("SELECT COUNT(*) FROM employees"))
            count  = result.scalar()  # .scalar() gets the single value
        print(f"✅ Source DB connected — {count} employees found")
    except Exception as e:
        print(f"❌ Source DB connection failed: {e}")

    # Test warehouse connection
    try:
        warehouse_engine = get_warehouse_engine()
        with warehouse_engine.connect() as conn:
            result = conn.execute(text("SELECT current_schema()"))
            schema = result.scalar()
        print(f"✅ Warehouse DB connected — current schema: {schema}")
    except Exception as e:
        print(f"❌ Warehouse DB connection failed: {e}")

    print("\n✅ Connection test complete!\n")


# ═══════════════════════════════════════════════════════════════
# Run directly to test — python utils/db_connections.py
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    create_warehouse_schema()
    test_connections()