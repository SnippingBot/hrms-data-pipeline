Multi-Tenant HRMS Data Pipeline

Overview

A production-grade, configuration-driven ELT pipeline designed to ingest, unify, and analyze employee data from disparate HRMS source systems. Built using PySpark and Delta Lake, this architecture standardizes fragmented schemas, enforces strict data quality rules, and normalizes financial metrics for cross-departmental analytics.

How to Run

1. Environment Setup
Ensure Apache Spark (with Hadoop binaries for Windows) and Delta Lake dependencies are configured.

pip install pyspark pandas numpy


2. Auto-Mapper (Part B)
Generate the configuration for the unknown HRMS C format using the LLM utility. (Note: Ensure the source Excel file is converted to CSV for PySpark ingestion).

python src/auto_mapper.py data/hrms_c_export.xlsx HRMS_C configs/hrms_c_proposed.json


3. Run Pipeline Ingestion (Part A & B)
Process the historical and incremental files:

python src/pipeline.py configs/hrms_a.json
python src/pipeline.py configs/hrms_b.json
python src/pipeline.py configs/hrms_c_approved.json
python src/pipeline.py configs/hrms_a_day2.json


4. Generate Analytics (Part C)
Run the analytics engine to produce the automated Markdown report and flag anomalies:

python src/analytics.py


Results will be saved in output/part_c_report.md and output/flagged_salaries.csv.

Validation Rules

Data quality is enforced during the ingestion phase. Rows violating these rules are filtered out to maintain analytical integrity:

Null Checks: source_employee_id, full_name, hire_date, grade, and annual_base_local must not be null.

Temporal Logic: hire_date <= CURRENT_DATE() (prevents future-dated hires).

Domain Integrity:

employment_status must resolve to strictly 'Active' or 'Terminated'.

annual_base_local must be strictly > 0 (no negative or zero salaries).

currency must resolve to standard ISO codes ('INR', 'AED', 'USD', 'GBP').

Supported Transformations

The pipeline engine supports dynamic SQL expressions inside the JSON configuration files, including:

Date Parsing: COALESCE with TO_DATE to handle various incoming formats (MM-dd-yyyy, dd/MM/yyyy, yyyy-MM-dd).

String Manipulation: TRIM, UPPER, LOWER, and LEFT (used to standardize gender indicators to 'M' or 'F').

Conditional Logic: CASE WHEN statements to normalize statuses (e.g., 'A' -> 'Active').

Column Splitting: SPLIT(col, '-')[x] to separate concatenated fields (e.g., extracting City and Country from "Pune - IN").

Mathematical Operations: Multiplying monthly CTC by 12 to derive annual base salaries.

Dynamic Imputation: Context-aware data cleaning (e.g., deriving currency from the employee's country location if the source currency is missing or invalid).

Design Choices & Assumptions

Compute & Storage: Chose PySpark and Delta Lake (Bronze/Silver architecture). Delta Lake's MERGE operation natively handles idempotent Day 2 loads, cleanly upserting hrms_a_day2 without duplicating records.

Configuration-Driven: Hardcoding transformations into Python creates technical debt. By offloading field mapping and SQL expressions to JSON files, the Python engine remains format-agnostic.

Generative AI for Schema Inference: Leveraged Google's Gemini LLM to parse unknown headers and propose a target JSON mapping, simulating a highly scalable onboarding workflow.

Analytical Normalization: Financial averages are mathematically meaningless across diverse local currencies. The analytics layer dynamically normalizes all salaries to a base USD rate before calculating the gender pay gap or standard deviation anomalies.

Data Findings & Anomalies

During ingestion, several data quality issues were identified and programmatically resolved:

Broken Date Formats: HRMS B utilized dd/MM/yyyy formats (e.g., 31/08/2017). The JSON configuration was updated to coalesce this format, preventing mass row rejections.

Invalid Currencies (XXX): Found dummy/invalid currency codes. Rather than rejecting these records, the pipeline uses a defensive CASE WHEN transformation to impute the correct currency based on the employee's country code (e.g., IN = INR).

Monthly vs. Annual Salaries: HRMS B provided monthly CTC. This was normalized to annual base pay using a multiplication transformation in the config.

Excel Source Files: HRMS C was provided as a binary .xlsx file, which crashes distributed Spark workers. It was manually converted to .csv to align with big-data best practices.

Scaling to 300 Customers

If this architecture needed to onboard 300 unique HRMS systems, I would implement the following architectural upgrades:

Managed Cloud Infrastructure: Migrate from local PySpark to Databricks or AWS EMR to handle massive concurrent data processing.

UI for the Auto-Mapper: Build a simple React/Streamlit frontend over the LLM Auto-Mapper. Implementation consultants could upload an Excel file, visually review the AI's proposed mapping, edit it, and click "Deploy", which would automatically save the JSON to a cloud storage bucket (S3/GCS).

Orchestration: Introduce Apache Airflow or Dagster to manage pipeline dependencies, automate the reconciliation checks against manifest.csv, and trigger alerts for high rejection rates.

Centralized Metadata: Move the JSON configuration files into a NoSQL database (like MongoDB or DynamoDB) for easier version control and querying across 300 tenants.

### Assumption: HRMS_C `Field_17`
In the HRMS_C dataset, `Field_17` lacks a descriptive header. Based on data profiling, it contains continuous numeric values ranging roughly from 40,000 to 120,000, which aligns perfectly with standard local annual compensation bands for the given roles. Since no other column in HRMS_C maps logically to salary, I have mapped `Field_17` to `annual_base_local` and treated it as the gross annual compensation.

Time Spent

Part A (Pipeline & Delta Lake Architecture): ~3.5 hours

Part B (Auto-Mapper Integration & HRMS C): ~2 hours

Part C (Analytics Automation & Reporting): ~1.5 hours

Part D (Testing) ~ 1 hour

Total: ~ 8 hours