# ============================================================
# FILE: 02_etl_pipeline/extract/extractor.py
# PURPOSE: Extract (read) raw data from the source PostgreSQL DB
# ============================================================

import sys
import os
from pathlib import Path

# ── Add project root to path so we can import from utils ─────

sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import text
from utils.db_connections import get_source_engine

# ── Initialize source engine once ────────────────────────────

source_engine = get_source_engine()

def read_sql(query):
    """Wrapper that handles pandas SQLAlchemy compatibility"""
    from sqlalchemy import text
    with source_engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

# ═══════════════════════════════════════════════════════════════
# FUNCTION 1 — extract_employees()
# Reads all employee records from source DB
# ═══════════════════════════════════════════════════════════════
def extract_employees():
    """
    Extracts all employee records from the source database.
    Joins with departments to get department_name inline.

    Returns:
        pandas DataFrame with all employee columns
    """
    query = """
        SELECT
            e.employee_id,
            e.first_name,
            e.last_name,
            e.email,
            e.hire_date,
            e.job_title,
            e.status,
            e.department_id,
            e.manager_id,
            e.created_at,
            d.department_name,   -- Joining dept name directly
            d.location           -- Joining location directly
        FROM employees e
        LEFT JOIN departments d
            ON e.department_id = d.department_id
        ORDER BY e.employee_id
    """

    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} employees")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 2 — extract_departments()
# ═══════════════════════════════════════════════════════════════
def extract_departments():
    """
    Extracts all department records.
    Joins with employees to get the manager's full name.

    Returns:
        pandas DataFrame with department + manager name
    """
    query = """
        SELECT
            d.department_id,
            d.department_name,
            d.location,
            d.manager_id,
            -- CONCAT manager name — will be NULL if no manager assigned
            CONCAT(e.first_name, ' ', e.last_name) AS manager_name,
            d.created_at
        FROM departments d
        LEFT JOIN employees e
            ON d.manager_id = e.employee_id
        ORDER BY d.department_id
    """
    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} departments")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 3 — extract_salaries()
# ═══════════════════════════════════════════════════════════════
def extract_salaries():
    """
    Extracts all salary records — including full history.
    We extract everything; transformer will filter current salaries.
    Returns:
        pandas DataFrame with all salary records
    """
    query = """
        SELECT
            salary_id,
            employee_id,
            amount,
            currency,
            effective_date,
            end_date,       -- NULL means current salary
            created_at
        FROM salaries
        ORDER BY employee_id, effective_date
    """
    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} salary records")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 4 — extract_performance_reviews()
# ═══════════════════════════════════════════════════════════════
def extract_performance_reviews():
    """
    Extracts all performance reviews.
    Used to calculate average rating per employee for fact table.

    Returns:
        pandas DataFrame with all review records
    """
    query = """
        SELECT
            review_id,
            employee_id,
            reviewer_id,
            rating,
            review_date,
            comments,
            created_at
        FROM performance_reviews
        ORDER BY employee_id, review_date
    """
    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} performance reviews")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 5 — extract_projects()
# ═══════════════════════════════════════════════════════════════
def extract_projects():
    """
    Extracts all project records with department name.

    Returns:
        pandas DataFrame with project details
    """
    query = """
        SELECT
            p.project_id,
            p.project_name,
            p.department_id,
            d.department_name,
            p.start_date,
            p.end_date,
            p.status,
            p.budget,
            p.created_at
        FROM projects p
        LEFT JOIN departments d
            ON p.department_id = d.department_id
        ORDER BY p.project_id
    """
    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} projects")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 6 — extract_employee_projects()
# ═══════════════════════════════════════════════════════════════
def extract_employee_projects():
    """
    Extracts employee-project assignments.
    Used in fact table to count active projects per employee.

    Returns:
        pandas DataFrame with assignment records
    """
    query = """
        SELECT
            emp_project_id,
            employee_id,
            project_id,
            role,
            assigned_date
        FROM employee_projects
        ORDER BY employee_id
    """
    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} project assignments")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 7 — extract_leave_requests()
# ═══════════════════════════════════════════════════════════════
def extract_leave_requests():
    """
    Extracts all leave requests.
    Used to calculate leave days taken per employee for fact table.

    Returns:
        pandas DataFrame with leave records
    """
    query = """
        SELECT
            leave_id,
            employee_id,
            leave_type,
            start_date,
            end_date,
            status,
            -- Pre-calculate duration in days here
            -- (end_date - start_date) gives an INTERVAL in PostgreSQL
            -- EXTRACT(DAY FROM ...) converts it to a plain number
            (end_date - start_date) AS leave_days,
            created_at
        FROM leave_requests
        WHERE status = 'approved'   -- Only count approved leaves
        ORDER BY employee_id, start_date
    """
    df = read_sql(query)
    print(f"  📥 Extracted {len(df)} approved leave requests")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 8 — extract_all()
# Master function — runs all extractions and returns everything
# This is what the pipeline.py will call
# ═══════════════════════════════════════════════════════════════
def extract_all():
    """
    Runs all extract functions and returns a dictionary of DataFrames.

    Returns:
        dict of {table_name: DataFrame}
    """
    print("\n🔄 EXTRACT PHASE — Reading from source database...\n")

    data = {
        'employees':    extract_employees(),
        'departments':  extract_departments(),
        'salaries':     extract_salaries(),
        'reviews':      extract_performance_reviews(),
        'projects':     extract_projects(),
        'emp_projects': extract_employee_projects(),
        'leaves':       extract_leave_requests()
    }

    # Print summary
    print(f"\n✅ Extract complete!")
    print(f"   Total tables extracted : {len(data)}")
    total_rows = sum(len(df) for df in data.values())
    print(f"   Total rows extracted   : {total_rows:,}")

    return data


# ═══════════════════════════════════════════════════════════════
# Run directly to test — python extract/extractor.py
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    data = extract_all()

    # Show first 3 rows of each table so you can verify
    print("\n📋 Sample data preview:\n")
    for table_name, df in data.items():
        print(f"  {table_name} — {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"  Columns: {list(df.columns)}")
        print()