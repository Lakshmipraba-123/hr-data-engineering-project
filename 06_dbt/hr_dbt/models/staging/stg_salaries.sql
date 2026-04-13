-- ============================================================
-- MODEL: staging/stg_salaries.sql
-- PURPOSE: Clean raw salaries — flag current salary
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('hr_source', 'salaries') }}
),

cleaned AS (
    SELECT
        salary_id,
        employee_id,
        amount          AS salary_amount,
        currency,
        effective_date,
        end_date,
        -- is_current = TRUE when end_date is NULL (no end = still active)
        CASE WHEN end_date IS NULL THEN TRUE ELSE FALSE END AS is_current,
        created_at
    FROM source
    WHERE employee_id IS NOT NULL
      AND amount > 0
)

SELECT * FROM cleaned