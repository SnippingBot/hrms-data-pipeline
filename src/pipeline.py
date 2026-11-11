import json
import sys
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\
    bin"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def build_spark_session():
    return SparkSession.builder \
        .appName("Compport_HRMS_Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

def write_to_delta(spark, df, table_name, join_key):
    delta_path = f"output/{table_name}"
    
    # If table doesn't exist, write it for the first time
    if not os.path.exists(delta_path):
        df.write.format("delta").mode("overwrite").save(delta_path)
    else:
        # If table exists (Day 2 load), perform an upsert (MERGE)
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target") \
            .merge(
                df.alias("source"),
                f"target.{join_key} = source.{join_key}"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

def run_pipeline(config_path):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    config = load_config(config_path)
    system_name = config["source_system"]
    print(f"--- Starting pipeline for {system_name} ---")
    
    # 1. Read Raw Data
    raw_df = spark.read.csv(config["source_file"], header=True, inferSchema=True)
    total_received = raw_df.count()
    
    # 2. Apply Dynamic Transformations
    mapped_df = raw_df
    for mapping in config["mappings"]:
        target_col = mapping["target_field"]
        sql_expr = mapping["expr"]
        mapped_df = mapped_df.withColumn(target_col, F.expr(sql_expr))
        
    # 3. Validation & Splitting Rejects
    # Combine all rules with AND
    valid_condition = " AND ".join(config["validation_rules"])
    
    valid_df = mapped_df.filter(valid_condition)
    reject_df = mapped_df.filter(f"NOT ({valid_condition})")
    
    total_loaded = valid_df.count()
    total_rejected = reject_df.count()
    
    # Save rejected rows to CSV for review
    if total_rejected > 0:
        reject_df.write.csv(f"output/rejects_{system_name}", mode="overwrite", header=True)
    
   # 4. Split into Target Tables (Employee and Salary)
    emp_cols = [m["target_field"] for m in config["mappings"] if m["target_table"] == "employee"]
    sal_cols = [m["target_field"] for m in config["mappings"] if m["target_table"] == "salary"]
    
    # Select only target columns AND deduplicate based on the primary key
    emp_df = valid_df.select(*list(set(emp_cols))).dropDuplicates(["source_employee_id"])
    sal_df = valid_df.select(*list(set(sal_cols))).dropDuplicates(["source_employee_id"])
    
    # 5. Write to Delta Lake (Idempotent Load)
    write_to_delta(spark, emp_df, "employee", "source_employee_id")
    write_to_delta(spark, sal_df, "salary", "source_employee_id")
    
    
    # 6. Output Reconciliation Report
    print("\n--- Reconciliation Report ---")
    print(f"Source System:    {system_name}")
    print(f"Rows Received:    {total_received}")
    print(f"Rows Loaded:      {total_loaded}")
    print(f"Rows Rejected:    {total_rejected}")
    print("-----------------------------\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline.py <path_to_config.json>")
        sys.exit(1)
        
    run_pipeline(sys.argv[1])