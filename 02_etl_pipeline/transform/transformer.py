# ============================================================
# FILE: 02_etl_pipeline/transform/transformer.py
# PURPOSE: Clean raw data and build Star Schema tables
# ============================================================

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import date, timedelta

def transform_dim_date(start_year=2015, end_year=2026):
    """Build date dimension — every date from start to end year"""
    print("  🔄 Building dim_date...")
    dates = pd.date_range(
        start=f"{start_year}-01-01",
        end=f"{end_year}-12-31",
        freq="D"
    )
    df = pd.DataFrame({"full_date": dates})
    df["date_key"]     = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"]         = df["full_date"].dt.year
    df["quarter"]      = df["full_date"].dt.quarter
    df["quarter_name"] = "Q" + df["quarter"].astype(str) + " " + df["year"].astype(str)
    df["month"]        = df["full_date"].dt.month
    df["month_name"]   = df["full_date"].dt.strftime("%B")
    df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)
    df["day_of_week"]  = df["full_date"].dt.dayofweek + 1  # 1=Monday
    df["day_name"]     = df["full_date"].dt.strftime("%A")
    df["is_weekend"]   = df["day_of_week"].isin([6, 7])
    df["is_holiday"]   = False
    print(f"  ✅ dim_date — {len(df)} rows")
    return df


def transform_dim_department(departments_df):
    """Build department dimension with SCD Type 2 columns"""
    print("  🔄 Building dim_department...")
    df = departments_df.copy()

    # Clean nulls
    df["manager_name"]   = df["manager_name"].fillna("Not Assigned")
    df["location"]       = df["location"].fillna("Unknown")

    # SCD Type 2 columns
    df["effective_date"] = date(2015, 1, 1)
    df["expiry_date"]    = date(9999, 12, 31)
    df["is_current"]     = True

    # Select only warehouse columns
    df = df[[
        "department_id", "department_name",
        "location", "manager_name",
        "effective_date", "expiry_date", "is_current"
    ]]
    print(f"  ✅ dim_department — {len(df)} rows")
    return df


def transform_dim_employee(employees_df):
    """Build employee dimension with SCD Type 2 columns"""
    print("  🔄 Building dim_employee...")
    df = employees_df.copy()

    # Clean nulls
    df["job_title"]      = df["job_title"].fillna("Unknown")
    df["department_name"]= df["department_name"].fillna("Unknown")

    # Build derived columns
    df["full_name"]      = df["first_name"] + " " + df["last_name"]
    df["hire_date"]      = pd.to_datetime(df["hire_date"])

    # Tenure in years — rounded to 2 decimals
    today = pd.Timestamp(date.today())
    df["tenure_years"]   = ((today - df["hire_date"]).dt.days / 365).round(2)

    # SCD Type 2 columns
    df["effective_date"] = date(2015, 1, 1)
    df["expiry_date"]    = date(9999, 12, 31)
    df["is_current"]     = True

    df = df[[
        "employee_id", "first_name", "last_name", "full_name",
        "email", "job_title", "department_name", "hire_date",
        "status", "tenure_years",
        "effective_date", "expiry_date", "is_current"
    ]]
    print(f"  ✅ dim_employee — {len(df)} rows")
    return df


def transform_dim_project(projects_df):
    """Build project dimension"""
    print("  🔄 Building dim_project...")
    df = projects_df.copy()

    df["start_date"]     = pd.to_datetime(df["start_date"])
    df["end_date"]       = pd.to_datetime(df["end_date"])
    df["duration_days"]  = (df["end_date"] - df["start_date"]).dt.days
    df["department_name"]= df["department_name"].fillna("Unknown")

    df = df[[
        "project_id", "project_name", "department_name",
        "status", "start_date", "end_date",
        "budget", "duration_days"
    ]]
    print(f"  ✅ dim_project — {len(df)} rows")
    return df


def transform_dim_leave_type():
    """Build leave type dimension — static reference data"""
    print("  🔄 Building dim_leave_type...")
    data = [
        {"leave_type": "annual",    "category": "personal", "is_paid": True},
        {"leave_type": "sick",      "category": "medical",  "is_paid": True},
        {"leave_type": "maternity", "category": "parental", "is_paid": True},
        {"leave_type": "paternity", "category": "parental", "is_paid": True},
        {"leave_type": "unpaid",    "category": "personal", "is_paid": False},
    ]
    df = pd.DataFrame(data)
    print(f"  ✅ dim_leave_type — {len(df)} rows")
    return df


def transform_fact_table(employees_df, salaries_df, reviews_df,
                          leaves_df, emp_projects_df,
                          dim_emp_df, dim_dept_df, dim_proj_df):
    """
    Build fact_employee_snapshot.
    One row per employee — snapshot of current state.
    """
    print("  🔄 Building fact_employee_snapshot...")

    # ── Current salary per employee (end_date IS NULL) ────────
    current_sal = salaries_df[salaries_df["end_date"].isna()][
        ["employee_id", "amount"]
    ].rename(columns={"amount": "salary_amount"})

    # ── Latest performance rating per employee ────────────────
    latest_rating = (
        reviews_df.sort_values("review_date")
        .groupby("employee_id")["rating"]
        .last()
        .reset_index()
        .rename(columns={"rating": "performance_rating"})
    )

    # ── Total approved leave days per employee ────────────────
    total_leaves = (
        leaves_df.groupby("employee_id")["leave_days"]
        .sum()
        .reset_index()
        .rename(columns={"leave_days": "leave_days_taken"})
    )

    # ── Project count per employee ────────────────────────────
    proj_count = (
        emp_projects_df.groupby("employee_id")["project_id"]
        .count()
        .reset_index()
        .rename(columns={"project_id": "project_count"})
    )

    # ── Build surrogate key lookups ───────────────────────────
    # dim_employee: employee_id → employee_key
    emp_key_map = dim_emp_df[["employee_id", "employee_key"]]

    # dim_department: department_id → department_key
    dept_key_map = dim_dept_df[["department_id", "department_key"]]

    # ── Start with employees as base ──────────────────────────
    fact = employees_df[["employee_id", "department_id", "status"]].copy()

    # Merge all measures
    fact = fact.merge(current_sal,   on="employee_id", how="left")
    fact = fact.merge(latest_rating, on="employee_id", how="left")
    fact = fact.merge(total_leaves,  on="employee_id", how="left")
    fact = fact.merge(proj_count,    on="employee_id", how="left")

    # Fill missing values with sensible defaults
    fact["leave_days_taken"]   = fact["leave_days_taken"].fillna(0).astype(int)
    fact["project_count"]      = fact["project_count"].fillna(0).astype(int)
    fact["performance_rating"] = fact["performance_rating"].fillna(0.0)
    fact["salary_amount"]      = fact["salary_amount"].fillna(0.0)
    fact["is_active"]          = fact["status"] == "active"

    # Add surrogate keys
    fact = fact.merge(emp_key_map,  on="employee_id",  how="left")
    fact = fact.merge(dept_key_map, on="department_id", how="left")

    # Add today's date_key (YYYYMMDD format)
    today_key = int(date.today().strftime("%Y%m%d"))
    fact["date_key"]    = today_key
    fact["project_key"] = 1  # Default — will refine in Airflow phase

    # Final columns for warehouse
    fact = fact[[
        "employee_key", "department_key", "date_key", "project_key",
        "salary_amount", "performance_rating",
        "leave_days_taken", "project_count", "is_active"
    ]]

    print(f"  ✅ fact_employee_snapshot — {len(fact)} rows")
    return fact


def transform_all(data):
    """
    Master transform function.
    Receives raw data dict from extractor, returns clean dict.
    """
    print("\n🔄 TRANSFORM PHASE — Cleaning and building Star Schema...\n")

    dim_date       = transform_dim_date()
    dim_department = transform_dim_department(data["departments"])
    dim_employee   = transform_dim_employee(data["employees"])
    dim_project    = transform_dim_project(data["projects"])
    dim_leave_type = transform_dim_leave_type()

    # Add temporary keys for fact table building
    dim_employee["employee_key"]    = range(1, len(dim_employee) + 1)
    dim_department["department_key"]= range(1, len(dim_department) + 1)
    dim_project["project_key"]      = range(1, len(dim_project) + 1)

    fact = transform_fact_table(
        data["employees"], data["salaries"],
        data["reviews"],   data["leaves"],
        data["emp_projects"],
        dim_employee, dim_department, dim_project
    )

    transformed = {
        "dim_date":       dim_date,
        "dim_department": dim_department,
        "dim_employee":   dim_employee,
        "dim_project":    dim_project,
        "dim_leave_type": dim_leave_type,
        "fact_employee_snapshot": fact
    }

    print(f"\n✅ Transform complete! {len(transformed)} tables ready to load.")
    return transformed


if __name__ == "__main__":
    from extract.extractor import extract_all
    raw  = extract_all()
    clean = transform_all(raw)
    for name, df in clean.items():
        print(f"  {name}: {df.shape[0]} rows × {df.shape[1]} cols")