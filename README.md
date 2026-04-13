# 🏗️ HR Data Engineering Platform

An end-to-end Data Engineering project built for portfolio and learning purposes — covering the full data pipeline from raw data generation to analytics dashboard.

---

## 📌 Project Overview

This project simulates a real-world HR data platform for a company with 500+ employees. It demonstrates the complete data engineering lifecycle:

```
Raw Data → Ingestion → Processing → Warehouse → Analytics → Dashboard
```

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     SOURCE LAYER                            │
│  PostgreSQL (hr_source_db) — 7 tables, 5,500+ rows         │
│  Generated using Python Faker library                       │
└──────────────────────┬──────────────────────────────────────┘
                       │ ETL Pipeline (Python)
┌──────────────────────▼──────────────────────────────────────┐
│                   WAREHOUSE LAYER                           │
│  PostgreSQL (hr_warehouse schema) — Star Schema             │
│  1 Fact table + 5 Dimension tables                         │
└──────────────────────┬──────────────────────────────────────┘
                       │ dbt transformations
┌──────────────────────▼──────────────────────────────────────┐
│                 TRANSFORMATION LAYER                        │
│  dbt models — Staging → Dimensions → Facts → Marts         │
│  Data quality tests + auto-generated documentation         │
└──────────────────────┬──────────────────────────────────────┘
                       │ PySpark analytics
┌──────────────────────▼──────────────────────────────────────┐
│                  ANALYTICS LAYER                            │
│  PySpark jobs — Salary analysis, Performance trends        │
│  Attrition risk scoring with rule-based ML                 │
└──────────────────────┬──────────────────────────────────────┘
                       │ Power BI
┌──────────────────────▼──────────────────────────────────────┐
│                  DASHBOARD LAYER                            │
│  Power BI — 5 pages: Overview, Salary, Performance,        │
│  Attrition Risk, Leave Analysis                            │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Tools |
|---|---|
| Source Database | PostgreSQL 16 |
| Data Generation | Python, Faker |
| ETL Pipeline | Python, SQLAlchemy, Pandas |
| Orchestration | Apache Airflow 2.8.1 |
| Big Data Processing | PySpark 3.5 |
| Data Warehouse | PostgreSQL (Star Schema) |
| Transformation | dbt Core 1.7 |
| Containerization | Docker |
| CI/CD | Jenkins |
| Dashboard | Power BI |
| Cloud | Azure Data Lake, Azure Data Factory |
| Streaming | Apache Kafka |
| Testing | pytest |
| Version Control | Git, GitHub |

---

## 📁 Project Structure

```
hr-data-engineering-project/
├── 01_data_modeling/
│   ├── generated_data/        ← 7 CSV files (500 employees)
│   └── sample_data/
│       └── generate_data.py   ← Faker data generator
│
├── 02_etl_pipeline/
│   ├── extract/
│   │   └── extractor.py       ← Reads from PostgreSQL source
│   ├── transform/
│   │   └── transformer.py     ← Builds Star Schema tables
│   ├── load/
│   │   └── loader.py          ← Writes to warehouse schema
│   ├── utils/
│   │   └── db_connections.py  ← Reusable DB connections
│   ├── tests/
│   │   └── test_etl.py        ← 9 pytest unit tests
│   └── pipeline.py            ← Master ETL runner
│
├── 03_airflow/
│   ├── dags/
│   │   └── hr_etl_dag.py      ← Airflow DAG (4 tasks)
│   └── docker-compose.yml     ← Airflow Docker setup
│
├── 04_pyspark/
│   └── jobs/
│       └── hr_spark_job.py    ← Salary, performance, attrition analysis
│
├── 06_dbt/
│   └── hr_dbt/
│       └── models/
│           ├── staging/       ← stg_employees, stg_salaries, stg_departments
│           ├── dimensions/    ← dim_employees, dim_departments
│           └── facts/         ← fct_employee_snapshot
│
├── 09_docker/
│   └── Dockerfile             ← Containerized ETL pipeline
│
├── 10_jenkins/
│   └── Jenkinsfile            ← 6-stage CI/CD pipeline
│
├── 11_powerbi/
│   └── hr_dashboard.pbix      ← Power BI dashboard
│
├── docs/
│   └── HR_Database_Reference.md
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🗄️ Data Model

### Source Schema (OLTP — PostgreSQL public schema)

7 normalized tables:
- `employees` — 500 records
- `departments` — 10 records
- `salaries` — 1,500+ records (salary history)
- `performance_reviews` — 1,700+ records
- `projects` — 15 records
- `employee_projects` — 88 records
- `leave_requests` — 1,700+ records

### Warehouse Schema (OLAP — Star Schema)

```
dim_employee ──────┐
dim_department ────┤
dim_date ──────────┼──► fact_employee_snapshot
dim_project ───────┤
dim_leave_type ────┘
```

---

## 🚀 How to Run

### Prerequisites
- Python 3.11+
- PostgreSQL 16
- Docker Desktop
- Power BI Desktop

### Setup

```bash
# Clone the repository
git clone https://github.com/Lakshmipraba-123/hr-data-engineering-project.git
cd hr-data-engineering-project

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Create .env file (see .env.example)
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### Generate Source Data

```bash
python 01_data_modeling/sample_data/generate_data.py
```

### Run ETL Pipeline

```bash
python 02_etl_pipeline/pipeline.py
```

### Run with Docker

```bash
docker build -t hr-etl-pipeline:latest -f 09_docker/Dockerfile .
docker run --rm \
  -e DB_HOST=<your-host> \
  -e DB_PORT=5432 \
  -e DB_NAME=hr_source_db \
  -e DB_USER=postgres \
  -e DB_PASSWORD=<your-password> \
  hr-etl-pipeline:latest
```

### Run dbt Models

```bash
cd 06_dbt/hr_dbt
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### Run Tests

```bash
pytest 02_etl_pipeline/tests/test_etl.py -v
```

### Run PySpark Analysis

```bash
python 04_pyspark/jobs/hr_spark_job.py
```

---

## 📊 Dashboard Pages

| Page | Key Visuals |
|---|---|
| Overview | Total employees, avg salary, avg rating, headcount by dept |
| Salary Analysis | Avg salary by dept, salary by job title, top earners |
| Performance | Rating by dept, rating distribution, top performers |
| Attrition Risk | High/medium/low risk employees, risk factors |
| Leave Analysis | Leave patterns by type and department |

---

## 🧪 Tests

9 unit tests covering:
- Dimension table column validation
- Full name construction logic
- NULL value handling
- Date dimension range and weekend flags
- Leave type reference data
- Duplicate ID detection
- Tenure calculation

```bash
pytest 02_etl_pipeline/tests/test_etl.py -v
# 9 passed
```

---

## 🔄 CI/CD Pipeline (Jenkins)

6-stage pipeline:
1. **Checkout** — Pull latest code from GitHub
2. **Install Dependencies** — pip install requirements
3. **Run Tests** — pytest with JUnit XML report
4. **Build Docker Image** — containerize the ETL
5. **Run ETL Pipeline** — execute full pipeline
6. **Run dbt Models** — transform and test warehouse

---

## 📚 Key Concepts Demonstrated

- **Data Modeling** — OLTP vs OLAP, Star Schema, SCD Type 2
- **ETL Design** — Extract/Transform/Load separation of concerns
- **Data Quality** — pytest unit tests + dbt schema tests
- **Orchestration** — Airflow DAGs with retry logic
- **Big Data** — PySpark distributed processing, Window functions
- **Containerization** — Docker multi-stage builds
- **CI/CD** — Jenkins automated pipeline
- **Analytics** — Power BI star schema reporting

---

## 👩‍💻 Author

**Lakshmi Praba R**
- 📧 lakshmipraba23.01@gmail.com
- 💼 [LinkedIn](https://linkedin.com)
- 🐙 [GitHub](https://github.com/Lakshmipraba-123)

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).