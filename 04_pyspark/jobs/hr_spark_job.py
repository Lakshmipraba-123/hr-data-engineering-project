# ============================================================
# FILE: 04_pyspark/jobs/hr_spark_job.py
# PURPOSE: Process HR data using PySpark
#          Demonstrates big data processing concepts
# ============================================================

import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Load environment variables ────────────────────────────────
load_dotenv(Path("C:/hr-data-engineering-project/.env"))

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "hr_source_db")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")

JDBC_URL    = f"jdbc:postgresql://127.0.0.1:{DB_PORT}/{DB_NAME}"
JDBC_PROPS  = {
    "user":                DB_USER,
    "password":            DB_PASSWORD,
    "driver":              "org.postgresql.Driver",
    "sslmode":             "disable",
    "loginTimeout":        "30",
    "connectTimeout":      "30",
    "socketTimeout":       "30",
    "preferQueryMode":     "simple"
}

# ── Initialize SparkSession ───────────────────────────────────
# SparkSession is the entry point to all PySpark operations
# In production this connects to a cluster
# Locally it runs in "local" mode using all CPU cores
spark = SparkSession.builder \
    .appName("HR Data Processing") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Reduce noisy logs — only show warnings
spark.sparkContext.setLogLevel("WARN")
print("✅ SparkSession initialized")


# ═══════════════════════════════════════════════════════════════
# FUNCTION 1 — read_table()
# Generic function to read any PostgreSQL table into Spark DF
# ═══════════════════════════════════════════════════════════════
def read_table(table_name):
    """Read a PostgreSQL table into a Spark DataFrame"""
    df = spark.read.jdbc(
        url        = JDBC_URL,
        table      = table_name,
        properties = JDBC_PROPS
    )
    print(f"  📥 Loaded {table_name}: {df.count()} rows")
    return df


# ═══════════════════════════════════════════════════════════════
# FUNCTION 2 — analyze_salary_distribution()
# Shows salary statistics by department
# ═══════════════════════════════════════════════════════════════
def analyze_salary_distribution(employees_df, salaries_df, departments_df):
    """
    Calculates salary statistics per department.
    Uses PySpark aggregations — identical to SQL GROUP BY
    but runs distributed across workers.
    """
    print("\n🔄 Analyzing salary distribution...")

    # Get current salaries only (end_date IS NULL)
    current_salaries = salaries_df.filter(
        F.col("end_date").isNull()
    )

    # Join employees → salaries → departments
    result = employees_df \
        .join(current_salaries, "employee_id", "left") \
        .join(departments_df, "department_id", "left") \
        .groupBy("department_name") \
        .agg(
            F.count("employee_id")           .alias("headcount"),
            F.round(F.avg("amount"), 2)      .alias("avg_salary"),
            F.round(F.min("amount"), 2)      .alias("min_salary"),
            F.round(F.max("amount"), 2)      .alias("max_salary"),
            F.round(F.stddev("amount"), 2)   .alias("salary_stddev")
        ) \
        .orderBy(F.desc("avg_salary"))

    print("\n📊 Salary Distribution by Department:")
    result.show(truncate=False)
    return result


# ═══════════════════════════════════════════════════════════════
# FUNCTION 3 — analyze_performance_trends()
# Uses Window functions — advanced PySpark concept
# ═══════════════════════════════════════════════════════════════
def analyze_performance_trends(employees_df, reviews_df):
    """
    Analyzes performance trends using Window functions.
    Window functions calculate values across related rows
    without collapsing them like GROUP BY does.
    Real-world use: running totals, rankings, lag/lead analysis
    """
    print("\n🔄 Analyzing performance trends...")

    # Window spec: partition by employee, order by date
    # This means: for each employee, look at their reviews in order
    window_spec = Window \
        .partitionBy("employee_id") \
        .orderBy("review_date")

    reviews_with_trend = reviews_df \
        .withColumn(
            # lag() gets the PREVIOUS row's value
            # Used to compare current rating vs last rating
            "previous_rating",
            F.lag("rating", 1).over(window_spec)
        ) \
        .withColumn(
            # Calculate improvement: positive = got better
            "rating_change",
            F.col("rating") - F.col("previous_rating")
        ) \
        .withColumn(
            # Rank each review within the employee's history
            "review_rank",
            F.rank().over(window_spec)
        )

    # Get latest review per employee
    latest_reviews = reviews_with_trend \
        .groupBy("employee_id") \
        .agg(
            F.round(F.avg("rating"), 1)        .alias("avg_rating"),
            F.round(F.avg("rating_change"), 2) .alias("avg_improvement"),
            F.count("review_id")               .alias("total_reviews")
        )

    # Join with employees
    result = employees_df \
        .join(latest_reviews, "employee_id", "left") \
        .select(
            "employee_id",
            "full_name",
            "job_title",
            "avg_rating",
            "avg_improvement",
            "total_reviews"
        ) \
        .orderBy(F.desc("avg_rating"))

    print("\n📊 Top 10 Performers:")
    result.show(10, truncate=False)
    return result


# ═══════════════════════════════════════════════════════════════
# FUNCTION 4 — detect_attrition_risk()
# ML-lite: rule-based attrition risk scoring
# ═══════════════════════════════════════════════════════════════
def detect_attrition_risk(employees_df, salaries_df, reviews_df, leaves_df):
    """
    Scores each employee's attrition risk based on:
    - Low performance rating
    - Below average salary
    - High leave days
    - Long tenure without promotion (same title for 3+ years)
    """
    print("\n🔄 Calculating attrition risk scores...")

    # Current salaries
    current_sal = salaries_df \
        .filter(F.col("end_date").isNull()) \
        .select("employee_id", "amount")

    # Average rating per employee
    avg_ratings = reviews_df \
        .groupBy("employee_id") \
        .agg(F.round(F.avg("rating"), 1).alias("avg_rating"))

    # Total leave days per employee
    total_leaves = leaves_df \
        .filter(F.col("status") == "approved") \
        .groupBy("employee_id") \
        .agg(F.sum(
            F.datediff(F.col("end_date"), F.col("start_date"))
        ).alias("total_leave_days"))

    # Calculate company average salary
    avg_salary = current_sal.agg(
        F.avg("amount").alias("company_avg_salary")
    ).collect()[0]["company_avg_salary"]

    # Build risk score
    result = employees_df \
        .join(current_sal,   "employee_id", "left") \
        .join(avg_ratings,   "employee_id", "left") \
        .join(total_leaves,  "employee_id", "left") \
        .withColumn("risk_score",
            # Each condition adds points to risk score
            # Higher score = higher risk of leaving
            (F.when(F.col("avg_rating")      < 3,              2).otherwise(0)) +
            (F.when(F.col("amount")          < float(avg_salary) * 0.8, 2).otherwise(0))  +
            (F.when(F.col("total_leave_days") > 20,             1).otherwise(0)) +
            (F.when(F.col("tenure_years")    > 5,               1).otherwise(0))
        ) \
        .withColumn("risk_level",
            F.when(F.col("risk_score") >= 4, "HIGH")
            .when(F.col("risk_score") >= 2,  "MEDIUM")
            .otherwise("LOW")
        ) \
        .filter(F.col("status") == "active") \
        .select(
            "employee_id", "full_name", "job_title",
            "avg_rating", "amount", "tenure_years",
            "risk_score", "risk_level"
        ) \
        .orderBy(F.desc("risk_score"))

    print("\n📊 Attrition Risk Summary:")
    result.groupBy("risk_level") \
          .count() \
          .orderBy("risk_level") \
          .show()

    print("\n🚨 High Risk Employees (Top 10):")
    result.filter(F.col("risk_level") == "HIGH").show(10, truncate=False)
    return result


# ═══════════════════════════════════════════════════════════════
# FUNCTION 5 — save_results()
# Save Spark results to CSV files
# ═══════════════════════════════════════════════════════════════
def save_results(df, filename):
    """Save Spark DataFrame to CSV via pandas"""
    import os
    os.makedirs("04_pyspark/output", exist_ok=True)
    output_path = f"04_pyspark/output/{filename}.csv"
    # Convert Spark DF to pandas then save — avoids Hadoop file system issue
    df.toPandas().to_csv(output_path, index=False)
    print(f"  💾 Saved → {output_path}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🚀 Starting PySpark HR Analysis...\n")

    # Read tables
    print("📥 Reading tables from PostgreSQL...")
    employees_df   = read_table("employees")
    salaries_df    = read_table("salaries")
    departments_df = read_table("departments")
    reviews_df     = read_table("performance_reviews")
    leaves_df      = read_table("leave_requests")

    # Add full_name column to employees
    employees_df = employees_df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    ).withColumn(
        "tenure_years",
        F.round(F.datediff(F.current_date(), F.col("hire_date")) / 365, 2)
    )

    # Run analyses
    salary_df     = analyze_salary_distribution(
                        employees_df, salaries_df, departments_df)
    performance_df = analyze_performance_trends(
                        employees_df, reviews_df)
    attrition_df   = detect_attrition_risk(
                        employees_df, salaries_df, reviews_df, leaves_df)

    # Save results
    print("\n💾 Saving results...")
    save_results(salary_df,      "salary_distribution")
    save_results(performance_df, "performance_trends")
    save_results(attrition_df,   "attrition_risk")

    spark.stop()
    print("\n✅ PySpark job complete!")