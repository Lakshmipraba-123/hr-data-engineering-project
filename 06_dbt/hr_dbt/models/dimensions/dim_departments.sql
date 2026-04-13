-- ============================================================
-- MODEL: dimensions/dim_departments.sql
-- PURPOSE: Final department dimension
-- ============================================================

WITH stg_departments AS (
    SELECT * FROM {{ ref('stg_departments') }}
),

stg_employees AS (
    SELECT * FROM {{ ref('stg_employees') }}
),

final AS (
    SELECT
        d.department_id,
        d.department_name,
        d.location,
        COALESCE(
            CONCAT(m.first_name, ' ', m.last_name),
            'Not Assigned'
        )                    AS manager_name,
        COUNT(e.employee_id) AS headcount,
        CURRENT_DATE         AS effective_date,
        '9999-12-31'::DATE   AS expiry_date,
        TRUE                 AS is_current
    FROM stg_departments d
    LEFT JOIN stg_employees m
        ON d.manager_id = m.employee_id
    LEFT JOIN stg_employees e
        ON d.department_id = e.department_id
    GROUP BY
        d.department_id, d.department_name,
        d.location, m.first_name, m.last_name
)

SELECT * FROM final