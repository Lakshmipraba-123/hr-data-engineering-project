-- ============================================================
-- MODEL: dimensions/dim_employees.sql
-- PURPOSE: Final employee dimension with SCD Type 2 columns
-- MATERIALIZED: table (persisted in warehouse)
-- ============================================================

WITH stg_employees AS (
    SELECT * FROM {{ ref('stg_employees') }}
),

stg_departments AS (
    SELECT * FROM {{ ref('stg_departments') }}
),

final AS (
    SELECT
        e.employee_id,
        e.first_name,
        e.last_name,
        e.full_name,
        e.email,
        e.job_title,
        e.hire_date,
        e.status,
        e.tenure_years,
        d.department_name,
        d.location,
        -- SCD Type 2 tracking columns
        CURRENT_DATE         AS effective_date,
        '9999-12-31'::DATE   AS expiry_date,
        TRUE                 AS is_current
    FROM stg_employees e
    LEFT JOIN stg_departments d
        ON e.department_id = d.department_id
)

SELECT * FROM final