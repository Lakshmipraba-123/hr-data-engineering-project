-- ============================================================
-- MODEL: facts/fct_employee_snapshot.sql
-- PURPOSE: Core fact table — one row per employee
--          All measures analysts need in one place
-- ============================================================

WITH employees AS (
    SELECT * FROM {{ ref('stg_employees') }}
),

salaries AS (
    SELECT * FROM {{ ref('stg_salaries') }}
    WHERE is_current = TRUE   -- Only current salary
),

departments AS (
    SELECT * FROM {{ ref('stg_departments') }}
),

reviews AS (
    SELECT
        employee_id,
        ROUND(AVG(rating), 1) AS avg_rating,
        MAX(review_date)      AS last_review_date
    FROM {{ source('hr_source', 'performance_reviews') }}
    GROUP BY employee_id
),

leaves AS (
    SELECT
        employee_id,
        SUM(end_date - start_date) AS total_leave_days
    FROM {{ source('hr_source', 'leave_requests') }}
    WHERE status = 'approved'
    GROUP BY employee_id
),

projects AS (
    SELECT
        employee_id,
        COUNT(project_id) AS project_count
    FROM {{ source('hr_source', 'employee_projects') }}
    GROUP BY employee_id
),

final AS (
    SELECT
        e.employee_id,
        e.full_name,
        e.job_title,
        e.hire_date,
        e.tenure_years,
        e.status,
        e.department_id,
        d.department_name,
        d.location,
        -- Salary measures
        COALESCE(s.salary_amount, 0)    AS current_salary,
        s.currency,
        -- Performance measures
        COALESCE(r.avg_rating, 0)       AS avg_performance_rating,
        r.last_review_date,
        -- Leave measures
        COALESCE(l.total_leave_days, 0) AS total_leave_days,
        -- Project measures
        COALESCE(p.project_count, 0)    AS project_count,
        -- Flags
        CASE WHEN e.status = 'active'
             THEN TRUE ELSE FALSE END   AS is_active,
        CURRENT_DATE                    AS snapshot_date
    FROM employees e
    LEFT JOIN departments  d ON e.department_id  = d.department_id
    LEFT JOIN salaries     s ON e.employee_id    = s.employee_id
    LEFT JOIN reviews      r ON e.employee_id    = r.employee_id
    LEFT JOIN leaves       l ON e.employee_id    = l.employee_id
    LEFT JOIN projects     p ON e.employee_id    = p.employee_id
)

SELECT * FROM final