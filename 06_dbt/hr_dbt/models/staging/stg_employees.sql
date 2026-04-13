-- ============================================================
-- MODEL: staging/stg_employees.sql
-- PURPOSE: Clean raw employees table — rename, cast, filter
-- MATERIALIZED: view (no storage cost — runs on every query)
-- ============================================================

WITH source AS (
    -- Pull raw data from source schema
    SELECT * FROM {{ source('hr_source', 'employees') }}
),

cleaned AS (
    SELECT
        employee_id,
        first_name,
        last_name,
        -- Build full name here so all downstream models use same logic
        CONCAT(first_name, ' ', last_name)  AS full_name,
        LOWER(email)                         AS email,  -- Standardize to lowercase
        hire_date,
        job_title,
        department_id,
        manager_id,
        status,
        created_at,
        updated_at,
        -- Derived column: tenure in years
        ROUND(
            (CURRENT_DATE - hire_date)/ 365.0, 2
        )                                    AS tenure_years
    FROM source
    -- Filter out any test/dummy records
    WHERE employee_id IS NOT NULL
      AND email IS NOT NULL
)

SELECT * FROM cleaned