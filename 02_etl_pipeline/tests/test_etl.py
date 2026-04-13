# ============================================================
# FILE: 02_etl_pipeline/tests/test_etl.py
# PURPOSE: Unit tests for ETL pipeline
#          Jenkins runs these on every code push
# ============================================================

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pytest
import pandas as pd
from datetime import date


# ═══════════════════════════════════════════════════════════════
# TEST GROUP 1 — Transform logic tests
# These test our transformer functions with mock data
# No database needed — fast and isolated
# ═══════════════════════════════════════════════════════════════

def test_dim_employee_has_required_columns():
    """Verify dim_employee transformation produces required columns"""
    from transform.transformer import transform_dim_employee

    # Create mock employee data
    mock_employees = pd.DataFrame({
        'employee_id':    [1, 2, 3],
        'first_name':     ['John', 'Jane', 'Bob'],
        'last_name':      ['Doe', 'Smith', 'Jones'],
        'email':          ['john@test.com', 'jane@test.com', 'bob@test.com'],
        'hire_date':      [date(2020, 1, 1), date(2021, 6, 1), date(2019, 3, 15)],
        'job_title':      ['Engineer', 'Analyst', 'Manager'],
        'department_id':  [1, 2, 1],
        'department_name':['Engineering', 'Analytics', 'Engineering'],
        'location':       ['NYC', 'LA', 'NYC'],
        'manager_id':     [None, 1, None],
        'status':         ['active', 'active', 'terminated'],
        'created_at':     [date(2020, 1, 1)] * 3,
        'updated_at':     [date(2020, 1, 1)] * 3,
    })

    result = transform_dim_employee(mock_employees)

    # Check required columns exist
    required_cols = [
        'employee_id', 'full_name', 'email',
        'job_title', 'hire_date', 'status',
        'tenure_years', 'is_current'
    ]
    for col in required_cols:
        assert col in result.columns, f"Missing column: {col}"


def test_dim_employee_full_name_built_correctly():
    """full_name should be first_name + space + last_name"""
    from transform.transformer import transform_dim_employee

    mock_employees = pd.DataFrame({
        'employee_id':    [1],
        'first_name':     ['Lakshmi'],
        'last_name':      ['Praba'],
        'email':          ['lakshmi@test.com'],
        'hire_date':      [date(2020, 1, 1)],
        'job_title':      ['Data Engineer'],
        'department_id':  [1],
        'department_name':['Engineering'],
        'location':       ['Chennai'],
        'manager_id':     [None],
        'status':         ['active'],
        'created_at':     [date(2020, 1, 1)],
        'updated_at':     [date(2020, 1, 1)],
    })

    result = transform_dim_employee(mock_employees)
    assert result['full_name'].iloc[0] == 'Lakshmi Praba'


def test_dim_employee_null_job_title_filled():
    """NULL job titles should be replaced with 'Unknown'"""
    from transform.transformer import transform_dim_employee

    mock_employees = pd.DataFrame({
        'employee_id':    [1],
        'first_name':     ['Test'],
        'last_name':      ['User'],
        'email':          ['test@test.com'],
        'hire_date':      [date(2020, 1, 1)],
        'job_title':      [None],           # NULL job title
        'department_id':  [1],
        'department_name':['Engineering'],
        'location':       ['NYC'],
        'manager_id':     [None],
        'status':         ['active'],
        'created_at':     [date(2020, 1, 1)],
        'updated_at':     [date(2020, 1, 1)],
    })

    result = transform_dim_employee(mock_employees)
    assert result['job_title'].iloc[0] == 'Unknown'


def test_dim_date_generates_correct_range():
    """dim_date should generate dates for specified year range"""
    from transform.transformer import transform_dim_date

    result = transform_dim_date(start_year=2024, end_year=2024)

    # 2024 is a leap year — should have 366 rows
    assert len(result) == 366
    assert result['year'].unique()[0] == 2024


def test_dim_date_weekend_flag():
    """is_weekend should be True for Saturday and Sunday"""
    from transform.transformer import transform_dim_date

    result = transform_dim_date(start_year=2024, end_year=2024)

    # Filter weekends
    weekends = result[result['is_weekend'] == True]
    # All weekend rows should be Saturday(6) or Sunday(7)
    assert all(weekends['day_of_week'].isin([6, 7]))


def test_dim_leave_type_has_five_types():
    """Leave type dimension should always have 5 types"""
    from transform.transformer import transform_dim_leave_type

    result = transform_dim_leave_type()
    assert len(result) == 5


def test_dim_leave_type_unpaid_is_not_paid():
    """Unpaid leave should have is_paid = False"""
    from transform.transformer import transform_dim_leave_type

    result = transform_dim_leave_type()
    unpaid = result[result['leave_type'] == 'unpaid']
    assert unpaid['is_paid'].iloc[0] == False


# ═══════════════════════════════════════════════════════════════
# TEST GROUP 2 — Data quality tests
# ═══════════════════════════════════════════════════════════════

def test_no_duplicate_employee_ids():
    """Employee IDs must be unique in dim_employee"""
    from transform.transformer import transform_dim_employee

    mock_employees = pd.DataFrame({
        'employee_id':    [1, 2, 3],
        'first_name':     ['A', 'B', 'C'],
        'last_name':      ['X', 'Y', 'Z'],
        'email':          ['a@t.com', 'b@t.com', 'c@t.com'],
        'hire_date':      [date(2020, 1, 1)] * 3,
        'job_title':      ['Engineer'] * 3,
        'department_id':  [1] * 3,
        'department_name':['Eng'] * 3,
        'location':       ['NYC'] * 3,
        'manager_id':     [None] * 3,
        'status':         ['active'] * 3,
        'created_at':     [date(2020, 1, 1)] * 3,
        'updated_at':     [date(2020, 1, 1)] * 3,
    })

    result = transform_dim_employee(mock_employees)
    assert result['employee_id'].nunique() == len(result)


def test_tenure_years_non_negative():
    """Tenure years should never be negative"""
    from transform.transformer import transform_dim_employee

    mock_employees = pd.DataFrame({
        'employee_id':    [1],
        'first_name':     ['Test'],
        'last_name':      ['User'],
        'email':          ['t@t.com'],
        'hire_date':      [date(2020, 1, 1)],
        'job_title':      ['Engineer'],
        'department_id':  [1],
        'department_name':['Eng'],
        'location':       ['NYC'],
        'manager_id':     [None],
        'status':         ['active'],
        'created_at':     [date(2020, 1, 1)],
        'updated_at':     [date(2020, 1, 1)],
    })

    result = transform_dim_employee(mock_employees)
    assert result['tenure_years'].iloc[0] >= 0