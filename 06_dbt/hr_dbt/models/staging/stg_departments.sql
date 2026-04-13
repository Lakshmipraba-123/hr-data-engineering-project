-- ============================================================
-- MODEL: staging/stg_departments.sql
-- PURPOSE: Clean raw departments table
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('hr_source', 'departments') }}
),

cleaned AS (
    SELECT
        department_id,
        department_name,
        COALESCE(location, 'Unknown') AS location,  -- Replace NULL with Unknown
        manager_id,
        created_at
    FROM source
    WHERE department_id IS NOT NULL
)

SELECT * FROM cleaned