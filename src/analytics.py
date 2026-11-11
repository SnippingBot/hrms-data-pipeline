import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:\\hadoop"

def run_analytics():
    spark = SparkSession.builder \
        .appName("HRMS_Analytics") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    try:
        emp_df = spark.read.format("delta").load("output/employee")
        sal_df = spark.read.format("delta").load("output/salary")
    except Exception as e:
        print(f"Error loading Delta tables: {e}")
        return

    unified_df = emp_df.join(sal_df, "source_employee_id", "inner")
    unified_df.createOrReplaceTempView("unified_hrms")

    # NEW STEP: Normalize currencies to USD using static conversion rates
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW normalized_hrms AS
        SELECT 
            *,
            CASE 
                WHEN currency = 'USD' THEN annual_base_local
                WHEN currency = 'INR' THEN annual_base_local / 84.0
                WHEN currency = 'AED' THEN annual_base_local / 3.67
                WHEN currency = 'GBP' THEN annual_base_local / 0.79
                ELSE annual_base_local 
            END AS annual_base_usd
        FROM unified_hrms
    """)

    print("\n========================================================")
    print("                 PART C: ANALYTICS REPORT                 ")
    print("========================================================\n")

    # Analysis 1: Gender Pay Gap by Department (Now in USD)
    print("--- 1. Gender Pay Gap by Department (Normalized to USD) ---")
    pay_gap_df = spark.sql("""
        SELECT 
            department,
            gender,
            COUNT(source_employee_id) AS employee_count,
            ROUND(AVG(annual_base_usd), 2) AS avg_annual_salary_usd
        FROM normalized_hrms
        WHERE gender IN ('M', 'F') AND department IS NOT NULL
        GROUP BY department, gender
        ORDER BY department, gender
    """)
    pay_gap_df.show(truncate=False)

    # Analysis 2: Salary Anomalies (Now in USD)
    print("--- 2. Salary Anomalies (High Outliers by Department) ---")
    anomalies_df = spark.sql("""
        WITH DepartmentStats AS (
            SELECT 
                department,
                AVG(annual_base_usd) as dept_avg_salary_usd
            FROM normalized_hrms
            GROUP BY department
        )
        SELECT 
            u.source_employee_id,
            u.full_name,
            u.department,
            u.grade,
            u.currency,
            u.annual_base_local,
            ROUND(u.annual_base_usd, 2) AS salary_usd,
            ROUND(d.dept_avg_salary_usd, 2) AS dept_avg_salary_usd,
            ROUND((u.annual_base_usd - d.dept_avg_salary_usd) / d.dept_avg_salary_usd * 100, 1) AS pct_above_avg
        FROM normalized_hrms u
        JOIN DepartmentStats d ON u.department = d.department
        WHERE u.annual_base_usd > (d.dept_avg_salary_usd * 1.5)
        ORDER BY pct_above_avg DESC
        LIMIT 10
    """)
    anomalies_df.show(truncate=False)

if __name__ == "__main__":
    run_analytics()