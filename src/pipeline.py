import json
import sys
import os
import tempfile
import traceback
from datetime import datetime

import pandas as pd

# Windows and Spark setup (must run before pyspark is imported)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + os.path.join("C:\\hadoop", "bin")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def build_spark_session():
    return (
        SparkSession.builder
        .appName("Compport_HRMS_Pipeline")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def read_source(spark, config):
    """Read a CSV directly, or bridge an Excel file through a temporary CSV."""
    path = config["source_file"]

    if path.lower().endswith((".xlsx", ".xls")):
        sheet = config.get("sheet_name", 0)  # default: first sheet (Master Sheet)
        raw = pd.read_excel(path, sheet_name=sheet, header=None, dtype=object)

        # Header row = first row that is at least 60 percent filled.
        # This skips the title rows and the blank gap above the real header.
        counts = raw.notna().sum(axis=1)
        header_row = int(counts[counts >= 0.6 * counts.max()].index[0])

        df = raw.iloc[header_row + 1:].copy()
        df.columns = [
            str(c).strip() if pd.notna(c) else f"col_{i}"
            for i, c in enumerate(raw.iloc[header_row])
        ]
        df = df.dropna(how="all")

        def clean(v):
            if pd.isna(v):
                return None
            if isinstance(v, (pd.Timestamp, datetime)):
                return v.strftime("%Y-%m-%d")
            if isinstance(v, float) and v.is_integer():
                return str(int(v))
            return str(v).strip()

        df = df.apply(lambda col: col.map(clean))

        tmp_csv = os.path.abspath(os.path.join(tempfile.mkdtemp(), "source.csv"))
        df.to_csv(tmp_csv, index=False, encoding="utf-8")
        print(f"Excel header found on row {header_row + 1}", flush=True)
        print("Columns found:", list(df.columns), flush=True)
        return spark.read.csv(tmp_csv, header=True, inferSchema=False)

    # Plain CSV: read everything as strings, mappings do the casting
    return spark.read.csv(path, header=True, inferSchema=False)


def write_to_delta(spark, df, table_name, join_key):
    delta_path = f"output/{table_name}"

    if not os.path.exists(delta_path):
        # First load: create the table
        df.write.format("delta").mode("overwrite").save(delta_path)
    else:
        # Later loads: upsert so reruns never create duplicates
        delta_table = DeltaTable.forPath(spark, delta_path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), f"target.{join_key} = source.{join_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def run_pipeline(config_path):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    config = load_config(config_path)
    system_name = config["source_system"]
    print(f"--- Starting pipeline for {system_name} ---", flush=True)

    # 1. Read raw data
    print("Reading source...", flush=True)
    raw_df = read_source(spark, config)
    total_received = raw_df.count()
    print(f"Rows received: {total_received}", flush=True)

    # 2. Apply dynamic transformations from the mapping file
    mapped_df = raw_df
    for mapping in config["mappings"]:
        mapped_df = mapped_df.withColumn(mapping["target_field"], F.expr(mapping["expr"]))

    # A mapping such as "NULL" produces a VOID column, which CSV and Delta cannot store.
    # Cast any such column to string so unmapped fields load as empty values.
    for col_name, col_type in mapped_df.dtypes:
        if col_type in ("void", "null"):
            mapped_df = mapped_df.withColumn(col_name, F.col(col_name).cast("string"))

    # 3. Validation and reject split
    # coalesce(..., false) makes rows with NULL rule results count as rejects
    # instead of silently disappearing from both outputs
    valid_condition = " AND ".join(f"({r})" for r in config["validation_rules"])
    passes = F.coalesce(F.expr(valid_condition), F.lit(False))

    valid_df = mapped_df.filter(passes)
    reject_df = mapped_df.filter(~passes)

    total_loaded = valid_df.count()
    total_rejected = reject_df.count()

    if total_rejected > 0:
        reject_df.write.csv(f"output/rejects_{system_name}", mode="overwrite", header=True)

    # 4. Split into target tables
    emp_cols = [m["target_field"] for m in config["mappings"] if m["target_table"] == "employee"]
    sal_cols = [m["target_field"] for m in config["mappings"] if m["target_table"] == "salary"]

    emp_df = valid_df.select(*dict.fromkeys(emp_cols)).dropDuplicates(["source_employee_id"])
    sal_df = valid_df.select(*dict.fromkeys(sal_cols)).dropDuplicates(["source_employee_id"])

    # 5. Write to Delta Lake (idempotent)
    write_to_delta(spark, emp_df, "employee", "source_employee_id")
    write_to_delta(spark, sal_df, "salary", "source_employee_id")

    # 6. Reconciliation summary
    print("\n--- Reconciliation Report ---")
    print(f"Source System:    {system_name}")
    print(f"Rows Received:    {total_received}")
    print(f"Rows Loaded:      {total_loaded}")
    print(f"Rows Rejected:    {total_rejected}")
    print("-----------------------------\n", flush=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline.py <path_to_config.json>")
        sys.exit(1)
    try:
        run_pipeline(sys.argv[1])
        print("Pipeline finished OK", flush=True)
    except BaseException:
        traceback.print_exc()
        sys.exit(1)