# hrms-data-pipeline
# Multi-Tenant HRMS Data Pipeline

## Overview
A production-grade, configuration-driven ELT pipeline designed to ingest, unify, and analyze employee data from disparate HRMS source systems. Built using PySpark and Delta Lake, this architecture standardizes fragmented schemas, enforces strict data quality rules, and normalizes financial metrics for cross-departmental analytics.

## Architecture & Technology Stack
* **Compute Engine:** Apache Spark (PySpark)
* **Storage Layer:** Delta Lake (Bronze/Silver Architecture)
* **Metadata Management:** JSON configuration files
* **AI Integration:** Google Gemini Generative AI (LLM)

## Key Engineering Features
* **Configuration-Driven Ingestion:** Eliminates hardcoded logic. New source systems are onboarded strictly via JSON mapping configurations, allowing for infinite scalability without altering the core Python codebase.
* **Idempotent Incremental Loads:** Utilizes Delta Lake's `MERGE` operation. The pipeline handles Day 2 incremental loads with `WHEN MATCHED UPDATE` and `WHEN NOT MATCHED INSERT`, preventing duplicate records during pipeline reruns.
* **LLM Auto-Mapper Tool:** Integrates a Generative AI script (`auto_mapper.py`) that programmatically parses unknown Excel/CSV headers and generates the exact target schema mapping in JSON, drastically reducing manual onboarding time.
* **Data Quality & Quarantine Routing:** Implements strict validation arrays. Records violating core business logic (e.g., negative salaries, future hire dates, invalid statuses) are automatically filtered out of the analytical tables and routed to a `rejects/` directory for audit.
* **Dynamic Currency Normalization:** Embeds a late-binding SQL view to standardize diverse local currencies (INR, GBP, AED) into USD, ensuring downstream statistical aggregations (averages, outliers) are mathematically sound.

## Execution Sequence
1. **Generate Mapping:** `python src/auto_mapper.py data/hrms_c_export.xlsx HRMS_C configs/hrms_c.json`
2. **Process Data:** `python src/pipeline.py configs/<config_name>.json`
3. **Generate Analytics:** `python src/analytics.py`