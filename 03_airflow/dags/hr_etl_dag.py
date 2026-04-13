# ============================================================
# FILE: C:\retail_analytics\dags\hr_etl_dag.py
# PURPOSE: Airflow DAG that orchestrates our HR ETL pipeline
#          Runs daily — Extract → Transform → Load
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# ── Add HR project to Python path ────────────────────────────
# Airflow runs inside Docker — we tell it where our ETL code is
# This path is the CONTAINER path (mapped from your Windows path)
sys.path.insert(0, "/opt/airflow/hr_project")

# ── Default arguments applied to every task ──────────────────
default_args = {
    "owner":            "lakshmi_praba",   # Your name — shows in UI
    "retries":          3,                 # Retry 3 times on failure
    "retry_delay":      timedelta(minutes=5), # Wait 5 min between retries
    "email_on_failure": False,             # Set True when you have email configured
    "depends_on_past":  False,             # Each run is independent
}

# ── Define the DAG ────────────────────────────────────────────
with DAG(
    dag_id          = "hr_etl_pipeline",       # Unique name — shows in UI
    default_args    = default_args,
    description     = "Daily HR ETL — PostgreSQL to Star Schema Warehouse",
    schedule        = "@daily",                # Runs at midnight every day
    start_date      = datetime(2024, 1, 1),    # Backfill start date
    catchup         = False,                   # Don't run missed historical runs
    tags            = ["hr", "etl", "warehouse"],  # Labels in UI
) as dag:

    # ── TASK 1 — Extract ──────────────────────────────────────
    def run_extract(**context):
        """
        Extracts all tables from source PostgreSQL.
        Pushes data to XCom so transform task can use it.
        XCom = Airflow's way of passing data between tasks.
        """
        from extract.extractor import extract_all
        raw_data = extract_all()

        # XCom can only store small data — store row counts as proof
        # For large data, we'd use S3/Azure — covered in Phase 5
        counts = {k: len(v) for k, v in raw_data.items()}
        context["ti"].xcom_push(key="row_counts", value=counts)
        print(f"✅ Extract complete: {counts}")
        return counts

    # ── TASK 2 — Transform ────────────────────────────────────
    def run_transform(**context):
        """
        Transforms raw data into Star Schema tables.
        Re-extracts data (stateless design — safer for retries).
        """
        from extract.extractor     import extract_all
        from transform.transformer import transform_all
        raw_data        = extract_all()
        transformed     = transform_all(raw_data)
        table_shapes    = {k: v.shape for k, v in transformed.items()}
        print(f"✅ Transform complete: {table_shapes}")
        return str(table_shapes)

    # ── TASK 3 — Load ─────────────────────────────────────────
    def run_load(**context):
        """
        Loads transformed data into warehouse schema.
        Full refresh — truncate and reload every run.
        """
        from extract.extractor     import extract_all
        from transform.transformer import transform_all
        from load.loader           import load_all
        raw_data    = extract_all()
        transformed = transform_all(raw_data)
        load_all(transformed)
        print("✅ Load complete — warehouse updated")

    # ── TASK 4 — Validate ─────────────────────────────────────
    def run_validation(**context):
        """
        Quick sanity check after load.
        Verifies row counts are above minimum thresholds.
        If validation fails — pipeline raises an error.
        """
        from utils.db_connections import get_warehouse_engine, WAREHOUSE_SCHEMA
        from sqlalchemy import text

        engine = get_warehouse_engine()
        checks = {
            "dim_employee":           100,   # Expect at least 100 employees
            "dim_department":         5,     # Expect at least 5 departments
            "fact_employee_snapshot": 100,   # Expect at least 100 fact rows
            "dim_date":               365,   # Expect at least 1 year of dates
        }

        with engine.connect() as conn:
            for table, min_rows in checks.items():
                result = conn.execute(text(
                    f"SELECT COUNT(*) FROM {WAREHOUSE_SCHEMA}.{table}"
                ))
                count = result.scalar()
                if count < min_rows:
                    raise ValueError(
                        f"❌ Validation failed: {table} has {count} rows "
                        f"(expected >= {min_rows})"
                    )
                print(f"  ✅ {table}: {count} rows — OK")

        print("✅ All validations passed!")

    # ── Wire up tasks ─────────────────────────────────────────
    # PythonOperator wraps a Python function as an Airflow task
    task_extract = PythonOperator(
        task_id         = "extract",
        python_callable = run_extract,
    )

    task_transform = PythonOperator(
        task_id         = "transform",
        python_callable = run_transform,
    )

    task_load = PythonOperator(
        task_id         = "load",
        python_callable = run_load,
    )

    task_validate = PythonOperator(
        task_id         = "validate",
        python_callable = run_validation,
    )

    # ── Set task dependencies ─────────────────────────────────
    # >> means "then" — extract first, then transform, then load, then validate
    task_extract >> task_transform >> task_load >> task_validate