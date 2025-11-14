<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10-blue?logo=python" />
  <img src="https://img.shields.io/badge/Prefect-3.0-blueviolet?logo=prefect" />
  <img src="https://img.shields.io/badge/DuckDB-0.10.2-yellow?logo=duckdb" />
  <img src="https://img.shields.io/badge/Pandas-2.1-green?logo=pandas" />
  <img src="https://img.shields.io/badge/LLM_Assisted-Enabled-orange?logo=openai" />
  <img src="https://img.shields.io/badge/Status-Operational-brightgreen?logo=github" />
  <img src="https://img.shields.io/github/license/NobelShaji/ai-assisted-etl" />
</p>


# AI-Assisted ETL Pipeline with DuckDB & Prefect

An end-to-end **AI-assisted ETL/ELT pipeline** that ingests NYC Taxi data, runs **LLM-style data cleaning & transformation logic**, validates schemas, and loads analytics- and ML-ready tables into **DuckDB**.

The goal of this project is to simulate how **modern ETL tools embed LLM/agent capabilities** to:
- Propose cleaning and transformation rules
- Generate reusable code
- Build features for analytics & ML
- Keep everything **local, free, and transparent**

> ✅ All components in this project are local & free: Python, DuckDB, Prefect, Pandas, Pandera.  
> No external LLM calls – the “LLM” is represented by a deterministic Python stub that logs prompts and returns code.

---

## 🔍 High-Level Overview

**Data flow:**

1. **Ingest (Raw → CSV)**  
   - Download sample NYC Yellow Taxi trips CSV into `data/raw/taxi_sample.csv`.

2. **Bronze Layer (Raw → Parquet)**  
   - Basic type coercion & persistence in `data/bronze/taxi_sample.parquet`.

3. **LLM-Assisted Transform (Bronze → Cleaned DataFrame)**  
   - A stub function (`suggest_cleaning_code`) mimics an LLM:
     - Logs the input schema + goal to `docs/llm_logs/`
     - Returns a Python code string with a `transform(df)` function
   - The flow `exec`s that code and applies `transform(df)` to the Bronze data.

4. **Silver Layer (Validated Parquet)**  
   - Schema validation with **pandera**.
   - Writes `data/silver/taxi_sample_clean.parquet`.
   - Loads into DuckDB as `silver.taxi_sample`.

5. **Gold Layer (Feature Table)**  
   - SQL in `sql/04_ml_features_materialized.sql` builds:
     - `gold.taxi_trip_features` with features like:
       - trip duration, speed, tip percentage
       - long-trip / high-fare flags
       - time-of-day buckets (morning/afternoon/evening/night)

6. **Analytics & Feature SQL**  
   - Example analytics & feature engineering queries:
     - `sql/01_analytics_examples.sql`
     - `sql/02_feature_engineering_examples.sql`
     - `sql/03_data_quality_checks.sql`

---

## 🧱 Tech Stack

- **Language:** Python 3.10
- **Orchestration:** [Prefect 3](https://docs.prefect.io/)
- **Warehouse / Query Engine:** [DuckDB](https://duckdb.org/)
- **DataFrame Engine:** pandas
- **Schema Validation:** pandera
- **Storage Layers:**
  - `data/raw`   → landing / raw files
  - `data/bronze` → lightly cleaned parquet
  - `data/silver` → validated & cleaned parquet
  - `gold.*` tables in DuckDB

---

## 🗂 Project Structure

```bash
ai-assisted-etl/
├── .gitignore
├── README.md
├── requirements.txt
├── data/
│   ├── .gitkeep
│   ├── raw/        # raw CSV download
│   ├── bronze/     # bronze parquet
│   ├── silver/     # silver parquet
│   └── gold/       # (optional local exports)
├── docs/
│   └── llm_logs/
│       └── .gitkeep   # JSON logs of "LLM" suggestions
├── orchestration/
│   ├── __init__.py
│   └── flow_ingest_transform.py  # main Prefect flow
├── sql/
│   ├── 01_analytics_examples.sql
│   ├── 02_feature_engineering_examples.sql
│   ├── 03_data_quality_checks.sql
│   └── 04_ml_features_materialized.sql
└── src/
    ├── __init__.py
    ├── config.py         # config & paths
    ├── ingest.py         # download/read/write helpers
    ├── llm_assist.py     # "LLM" stub that returns cleaning code
    ├── validate.py       # pandera schema
    └── warehouse.py      # DuckDB connection & schema helpers

