# ============================================================
# FILE: 01_data_modeling/sample_data/generate_data.py
# PURPOSE: Generate realistic HR data using Faker library
#          and load it directly into PostgreSQL
# ============================================================

import os
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine, text

# ── Load .env file ────────────────────────────────────────────
dotenv_path = Path("C:/hr-data-engineering-project/.env")
load_dotenv(dotenv_path=dotenv_path)

# ── Debug check ───────────────────────────────────────────────
print(f"DB_HOST    : {os.getenv('DB_HOST')}")
print(f"DB_PORT    : {os.getenv('DB_PORT')}")
print(f"DB_NAME    : {os.getenv('DB_NAME')}")
print(f"DB_USER    : {os.getenv('DB_USER')}")
print(f"DB_PASSWORD: {'*****' if os.getenv('DB_PASSWORD') else 'NOT FOUND'}")

# ── Read config ───────────────────────────────────────────────
DB_HOST     = os.getenv('DB_HOST',     'localhost')
DB_PORT     = os.getenv('DB_PORT',     '5432')
DB_NAME     = os.getenv('DB_NAME',     'hr_source_db')
DB_USER     = os.getenv('DB_USER',     'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin123')

NUM_EMPLOYEES   = int(os.getenv('NUM_EMPLOYEES',   '500'))
NUM_DEPARTMENTS = int(os.getenv('NUM_DEPARTMENTS', '10'))

# ── Database connection ───────────────────────────────────────
DB_URL = (
    f"postgresql+psycopg2://"
    f"{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}"
    f"/{DB_NAME}"
)

engine = create_engine(DB_URL, echo=False)
print("✅ Connected to PostgreSQL successfully")

# ── Initialize Faker ──────────────────────────────────────────
fake = Faker('en_US')
Faker.seed(42)
random.seed(42)


# ═══════════════════════════════════════════════════════════════
# STEP 1 — Create Schema
# ═══════════════════════════════════════════════════════════════
def create_schema():
    schema_sql = """
    DROP TABLE IF EXISTS employee_projects CASCADE;
    DROP TABLE IF EXISTS leave_requests CASCADE;
    DROP TABLE IF EXISTS performance_reviews CASCADE;
    DROP TABLE IF EXISTS salaries CASCADE;
    DROP TABLE IF EXISTS employees CASCADE;
    DROP TABLE IF EXISTS projects CASCADE;
    DROP TABLE IF EXISTS departments CASCADE;

    CREATE TABLE departments (
        department_id   SERIAL PRIMARY KEY,
        department_name VARCHAR(100) NOT NULL,
        location        VARCHAR(100),
        manager_id      INT,
        created_at      TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE employees (
        employee_id   SERIAL PRIMARY KEY,
        first_name    VARCHAR(50) NOT NULL,
        last_name     VARCHAR(50) NOT NULL,
        email         VARCHAR(100) UNIQUE NOT NULL,
        hire_date     DATE NOT NULL,
        job_title     VARCHAR(100),
        department_id INT REFERENCES departments(department_id),
        manager_id    INT REFERENCES employees(employee_id),
        status        VARCHAR(20) DEFAULT 'active',
        created_at    TIMESTAMP DEFAULT NOW(),
        updated_at    TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE salaries (
        salary_id      SERIAL PRIMARY KEY,
        employee_id    INT NOT NULL REFERENCES employees(employee_id),
        amount         DECIMAL(12,2) NOT NULL,
        currency       VARCHAR(3) DEFAULT 'USD',
        effective_date DATE NOT NULL,
        end_date       DATE,
        created_at     TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE performance_reviews (
        review_id   SERIAL PRIMARY KEY,
        employee_id INT NOT NULL REFERENCES employees(employee_id),
        reviewer_id INT REFERENCES employees(employee_id),
        rating      INT CHECK (rating BETWEEN 1 AND 5),
        review_date DATE NOT NULL,
        comments    TEXT,
        created_at  TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE projects (
        project_id    SERIAL PRIMARY KEY,
        project_name  VARCHAR(200) NOT NULL,
        department_id INT REFERENCES departments(department_id),
        start_date    DATE,
        end_date      DATE,
        status        VARCHAR(20) DEFAULT 'active',
        budget        DECIMAL(15,2),
        created_at    TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE employee_projects (
        emp_project_id SERIAL PRIMARY KEY,
        employee_id    INT NOT NULL REFERENCES employees(employee_id),
        project_id     INT NOT NULL REFERENCES projects(project_id),
        role           VARCHAR(100),
        assigned_date  DATE DEFAULT CURRENT_DATE,
        UNIQUE(employee_id, project_id)
    );

    CREATE TABLE leave_requests (
        leave_id    SERIAL PRIMARY KEY,
        employee_id INT NOT NULL REFERENCES employees(employee_id),
        leave_type  VARCHAR(50) NOT NULL,
        start_date  DATE NOT NULL,
        end_date    DATE NOT NULL,
        status      VARCHAR(20) DEFAULT 'approved',
        created_at  TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()
    print("✅ Schema created successfully")


# ═══════════════════════════════════════════════════════════════
# STEP 2 — Generate Departments
# ═══════════════════════════════════════════════════════════════
def generate_departments():
    dept_names = [
        'Engineering', 'Data & Analytics', 'Human Resources',
        'Finance', 'Product Management', 'Marketing',
        'Sales', 'Customer Success', 'Legal', 'Operations'
    ]
    locations = [
        'San Francisco', 'New York', 'Austin', 'Chicago',
        'Seattle', 'Boston', 'Remote', 'London', 'Bangalore', 'Toronto'
    ]
    departments = []
    for name in dept_names[:NUM_DEPARTMENTS]:
        departments.append({
            'department_name': name,
            'location':        random.choice(locations),
            'manager_id':      None
        })
    df = pd.DataFrame(departments)
    df.to_sql('departments', engine, if_exists='append', index=False)
    print(f"✅ Generated {len(departments)} departments")
    return dept_names[:NUM_DEPARTMENTS]


# ═══════════════════════════════════════════════════════════════
# STEP 3 — Generate Employees
# ═══════════════════════════════════════════════════════════════
def generate_employees(dept_names):
    job_titles = {
        'Engineering':        ['Junior Developer', 'Software Engineer',
                               'Senior Engineer', 'Tech Lead', 'Engineering Manager'],
        'Data & Analytics':   ['Data Analyst', 'Data Engineer', 'Senior Data Engineer',
                               'Analytics Engineer', 'Data Science Manager'],
        'Human Resources':    ['HR Coordinator', 'HR Specialist', 'HR Business Partner',
                               'Talent Acquisition', 'HR Manager'],
        'Finance':            ['Financial Analyst', 'Accountant', 'Senior Analyst',
                               'Finance Manager', 'CFO'],
        'Product Management': ['Associate PM', 'Product Manager', 'Senior PM',
                               'Group PM', 'VP of Product'],
        'Marketing':          ['Marketing Coordinator', 'Content Strategist',
                               'Growth Marketer', 'Marketing Manager', 'CMO'],
        'Sales':              ['Sales Development Rep', 'Account Executive',
                               'Senior AE', 'Sales Manager', 'VP of Sales'],
        'Customer Success':   ['CS Specialist', 'Customer Success Manager',
                               'Senior CSM', 'CS Team Lead'],
        'Legal':              ['Legal Counsel', 'Senior Counsel', 'Legal Director'],
        'Operations':         ['Ops Coordinator', 'Operations Analyst',
                               'Operations Manager', 'COO']
    }
    employees  = []
    emails_used = set()

    for i in range(NUM_EMPLOYEES):
        dept_id   = random.randint(1, NUM_DEPARTMENTS)
        dept_name = dept_names[dept_id - 1]
        first     = fake.first_name()
        last      = fake.last_name()

        base_email = f"{first.lower()}.{last.lower()}@hrcompany.com"
        email      = base_email
        counter    = 1
        while email in emails_used:
            email = f"{first.lower()}.{last.lower()}{counter}@hrcompany.com"
            counter += 1
        emails_used.add(email)

        hire_date = fake.date_between(
            start_date=date(2015, 1, 1),
            end_date=date(2024, 1, 1)
        )
        titles    = job_titles.get(dept_name, ['Specialist', 'Manager', 'Analyst'])
        job_title = random.choice(titles)
        status    = random.choices(
            ['active', 'terminated'], weights=[95, 5]
        )[0]

        employees.append({
            'first_name':    first,
            'last_name':     last,
            'email':         email,
            'hire_date':     hire_date,
            'job_title':     job_title,
            'department_id': dept_id,
            'manager_id':    None,
            'status':        status
        })

    df = pd.DataFrame(employees)
    df.to_sql('employees', engine, if_exists='append', index=False)
    print(f"✅ Generated {NUM_EMPLOYEES} employees")


# ═══════════════════════════════════════════════════════════════
# STEP 4 — Assign Managers
# ═══════════════════════════════════════════════════════════════
def assign_managers():
    with engine.connect() as conn:
        depts = conn.execute(text(
            "SELECT department_id FROM departments"
        )).fetchall()

        for dept in depts:
            dept_id = dept[0]
            emps = conn.execute(text(
                "SELECT employee_id FROM employees WHERE department_id = :d LIMIT 20"
            ), {'d': dept_id}).fetchall()

            if len(emps) >= 2:
                manager_id = emps[0][0]
                conn.execute(text(
                    "UPDATE departments SET manager_id = :m WHERE department_id = :d"
                ), {'m': manager_id, 'd': dept_id})
                for emp in emps[1:]:
                    conn.execute(text(
                        "UPDATE employees SET manager_id = :m WHERE employee_id = :e"
                    ), {'m': manager_id, 'e': emp[0]})
        conn.commit()
    print("✅ Managers assigned")


# ═══════════════════════════════════════════════════════════════
# STEP 5 — Generate Salaries
# ═══════════════════════════════════════════════════════════════
def generate_salaries():
    salary_ranges = {
        'Junior':    (55000,  80000),
        'Associate': (60000,  85000),
        'Analyst':   (65000,  95000),
        'Engineer':  (90000, 140000),
        'Senior':    (110000, 160000),
        'Lead':      (130000, 180000),
        'Manager':   (120000, 175000),
        'Director':  (160000, 220000),
        'VP':        (200000, 300000),
        'CFO':       (250000, 400000),
        'CMO':       (250000, 400000),
        'COO':       (250000, 400000),
    }

    with engine.connect() as conn:
        employees = conn.execute(text(
            "SELECT employee_id, job_title, hire_date FROM employees"
        )).fetchall()

    salaries = []
    for emp in employees:
        emp_id, job_title, hire_date = emp
        base_min, base_max = 60000, 100000
        for keyword, (sal_min, sal_max) in salary_ranges.items():
            if keyword.lower() in job_title.lower():
                base_min, base_max = sal_min, sal_max
                break

        tenure_years = (date.today() - hire_date).days // 365
        num_changes  = min(tenure_years + 1, 3)
        current_date = hire_date

        for j in range(num_changes):
            amount   = round(random.uniform(base_min, base_max) * (1 + j * 0.08), 2)
            is_last  = (j == num_changes - 1)
            end_date = None if is_last else (
                current_date + timedelta(days=random.randint(365, 730))
            )
            salaries.append({
                'employee_id':    emp_id,
                'amount':         amount,
                'currency':       'USD',
                'effective_date': current_date,
                'end_date':       end_date
            })
            if end_date:
                current_date = end_date + timedelta(days=1)

    df = pd.DataFrame(salaries)
    df.to_sql('salaries', engine, if_exists='append', index=False)
    print(f"✅ Generated {len(salaries)} salary records")


# ═══════════════════════════════════════════════════════════════
# STEP 6 — Generate Performance Reviews
# ═══════════════════════════════════════════════════════════════
def generate_performance_reviews():
    review_comments = {
        5: ["Outstanding performance this year", "Exceeded all targets",
            "Exceptional leadership and delivery"],
        4: ["Strong contributor to the team", "Met and exceeded most goals",
            "Great collaboration and output"],
        3: ["Solid performance, meets expectations", "Good work, room to grow",
            "Consistent and reliable"],
        2: ["Below expectations in some areas", "Needs improvement in delivery",
            "Struggled with deadlines this cycle"],
        1: ["Significant performance issues identified", "Did not meet key targets",
            "Requires a performance improvement plan"]
    }

    with engine.connect() as conn:
        employees = conn.execute(text(
            "SELECT employee_id, hire_date, manager_id FROM employees"
        )).fetchall()

    reviews = []
    for emp in employees:
        emp_id, hire_date, manager_id = emp
        years_employed = (date.today() - hire_date).days // 365

        for year_offset in range(min(years_employed, 5)):
            review_year = date.today().year - year_offset
            review_date = date(review_year, 12, 15)
            if review_date > date.today():
                continue
            rating = random.choices(
                [1, 2, 3, 4, 5], weights=[2, 8, 35, 40, 15]
            )[0]
            reviews.append({
                'employee_id': emp_id,
                'reviewer_id': manager_id,
                'rating':      rating,
                'review_date': review_date,
                'comments':    random.choice(review_comments[rating])
            })

    df = pd.DataFrame(reviews)
    df.to_sql('performance_reviews', engine, if_exists='append', index=False)
    print(f"✅ Generated {len(reviews)} performance reviews")


# ═══════════════════════════════════════════════════════════════
# STEP 7 — Generate Projects and Assignments
# ═══════════════════════════════════════════════════════════════
def generate_projects():
    project_templates = [
        "Data Warehouse Migration", "Customer Portal Redesign",
        "Sales Dashboard", "HR Analytics Platform", "Mobile App v2",
        "API Gateway Implementation", "Cloud Migration",
        "Security Audit", "ERP Integration", "ML Recommendation Engine",
        "Data Lake Setup", "CI/CD Pipeline", "Employee Self-Service Portal",
        "Financial Reporting Tool", "Kubernetes Migration"
    ]

    projects = []
    for i in range(15):
        start    = fake.date_between(start_date=date(2020, 1, 1),
                                     end_date=date(2023, 6, 1))
        duration = random.randint(90, 540)
        end      = start + timedelta(days=duration)
        status   = 'completed' if end < date.today() else random.choice(
            ['active', 'active', 'active', 'on_hold']
        )
        projects.append({
            'project_name':  project_templates[i],
            'department_id': random.randint(1, NUM_DEPARTMENTS),
            'start_date':    start,
            'end_date':      end,
            'status':        status,
            'budget':        round(random.uniform(50000, 2000000), 2)
        })

    df = pd.DataFrame(projects)
    df.to_sql('projects', engine, if_exists='append', index=False)

    with engine.connect() as conn:
        emp_ids  = [r[0] for r in conn.execute(
            text("SELECT employee_id FROM employees")).fetchall()]
        proj_ids = [r[0] for r in conn.execute(
            text("SELECT project_id FROM projects")).fetchall()]

    roles       = ['Lead', 'Contributor', 'Reviewer', 'Stakeholder', 'Developer']
    assignments = []
    seen        = set()

    for proj_id in proj_ids:
        team_size = random.randint(3, 8)
        team      = random.sample(emp_ids, min(team_size, len(emp_ids)))
        for emp_id in team:
            if (emp_id, proj_id) not in seen:
                seen.add((emp_id, proj_id))
                assignments.append({
                    'employee_id':   emp_id,
                    'project_id':    proj_id,
                    'role':          random.choice(roles),
                    'assigned_date': fake.date_between(
                        start_date=date(2020, 1, 1), end_date=date.today()
                    )
                })

    df2 = pd.DataFrame(assignments)
    df2.to_sql('employee_projects', engine, if_exists='append', index=False)
    print(f"✅ Generated {len(projects)} projects, {len(assignments)} assignments")


# ═══════════════════════════════════════════════════════════════
# STEP 8 — Generate Leave Requests
# ═══════════════════════════════════════════════════════════════
def generate_leave_requests():
    leave_types   = ['annual', 'sick', 'maternity', 'paternity', 'unpaid']
    leave_weights = [60, 25, 5, 5, 5]

    with engine.connect() as conn:
        employees = conn.execute(text(
            "SELECT employee_id FROM employees"
        )).fetchall()

    leaves = []
    for emp in employees:
        emp_id     = emp[0]
        num_leaves = random.randint(2, 6)
        for _ in range(num_leaves):
            leave_type = random.choices(leave_types, weights=leave_weights)[0]
            start      = fake.date_between(
                start_date=date(2020, 1, 1), end_date=date(2024, 6, 1)
            )
            duration_map = {
                'annual':    (3, 15),
                'sick':      (1, 5),
                'maternity': (60, 90),
                'paternity': (10, 15),
                'unpaid':    (5, 30)
            }
            min_d, max_d = duration_map[leave_type]
            end = start + timedelta(days=random.randint(min_d, max_d))
            leaves.append({
                'employee_id': emp_id,
                'leave_type':  leave_type,
                'start_date':  start,
                'end_date':    end,
                'status':      random.choices(
                    ['approved', 'rejected', 'pending'],
                    weights=[85, 10, 5]
                )[0]
            })

    df = pd.DataFrame(leaves)
    df.to_sql('leave_requests', engine, if_exists='append', index=False)
    print(f"✅ Generated {len(leaves)} leave requests")


# ═══════════════════════════════════════════════════════════════
# STEP 9 — Export to CSV
# ═══════════════════════════════════════════════════════════════
def export_to_csv():
    tables = ['departments', 'employees', 'salaries',
              'performance_reviews', 'projects',
              'employee_projects', 'leave_requests']

    os.makedirs('01_data_modeling/generated_data', exist_ok=True)

    for table in tables:
        df   = pd.read_sql(f"SELECT * FROM {table}", engine)
        path = f"01_data_modeling/generated_data/{table}.csv"
        df.to_csv(path, index=False)
        print(f"  📄 {table}.csv — {len(df)} rows saved")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🚀 Starting HR data generation...\n")

    create_schema()
    dept_names = generate_departments()
    generate_employees(dept_names)
    assign_managers()
    generate_salaries()
    generate_performance_reviews()
    generate_projects()
    generate_leave_requests()

    print("\n📦 Exporting to CSV files...")
    export_to_csv()

    print("\n🎉 Done! Your HR database is ready.")
    print(f"   Database : hr_source_db")
    print(f"   Employees: {NUM_EMPLOYEES}")
    print(f"   Tables   : 7")
    print(f"   CSVs     : 01_data_modeling/generated_data/")