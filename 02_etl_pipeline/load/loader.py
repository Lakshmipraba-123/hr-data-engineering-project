# ============================================================
# FILE: 02_etl_pipeline/load/loader.py
# PURPOSE: Load transformed data into warehouse schema
# ============================================================

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connections import get_warehouse_engine, get_source_engine, WAREHOUSE_SCHEMA
from sqlalchemy import text

warehouse_engine = get_warehouse_engine()
source_engine    = get_source_engine()


def truncate_tables():
    print("  🗑️  Truncating existing warehouse tables...")
    with source_engine.begin() as conn:
        conn.execute(text(f"""
            TRUNCATE TABLE
                {WAREHOUSE_SCHEMA}.fact_employee_snapshot,
                {WAREHOUSE_SCHEMA}.dim_employee,
                {WAREHOUSE_SCHEMA}.dim_department,
                {WAREHOUSE_SCHEMA}.dim_project,
                {WAREHOUSE_SCHEMA}.dim_leave_type,
                {WAREHOUSE_SCHEMA}.dim_date
            RESTART IDENTITY CASCADE;
        """))
    print("  ✅ Tables cleared")

def load_table(df, table_name):
    """
    Generic loader — writes any DataFrame to warehouse.
    Uses connection object for pandas 2.x compatibility.
    """
    cols_to_drop = ["employee_key", "department_key", "project_key"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    with warehouse_engine.begin() as conn:
        df.to_sql(
            name      = table_name,
            con       = conn,
            schema    = WAREHOUSE_SCHEMA,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000
        )
    print(f"  ✅ Loaded {len(df):>6,} rows → {WAREHOUSE_SCHEMA}.{table_name}")


def load_all(transformed_data):
    """
    Master load function.
    Loads dimensions first, then fact table.
    Order is critical — fact table has FKs pointing to dims.
    """
    print("\n🔄 LOAD PHASE — Writing to warehouse...\n")

    truncate_tables()
    print()

    # Load dimensions first
    load_table(transformed_data["dim_date"],       "dim_date")
    load_table(transformed_data["dim_department"],  "dim_department")
    load_table(transformed_data["dim_employee"],    "dim_employee")
    load_table(transformed_data["dim_project"],     "dim_project")
    load_table(transformed_data["dim_leave_type"],  "dim_leave_type")

    # Load fact table last
    load_table(transformed_data["fact_employee_snapshot"], "fact_employee_snapshot")

    print(f"\n✅ Load complete! All tables written to '{WAREHOUSE_SCHEMA}' schema.")


if __name__ == "__main__":
    from extract.extractor import extract_all
    from transform.transformer import transform_all
    raw         = extract_all()
    transformed = transform_all(raw)
    load_all(transformed)