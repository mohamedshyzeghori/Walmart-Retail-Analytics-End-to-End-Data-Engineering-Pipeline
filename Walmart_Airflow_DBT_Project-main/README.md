# Walmart Retail Analytics — Data Engineering Pipeline

> A hands-on data engineering project demonstrating end-to-end ELT workflow using Apache Airflow, dbt, and Databricks Delta Lake. Built to practice production patterns: incremental processing, data quality gates, SCD Type-2 tracking, and containerized orchestration.

---

## What This Project Is

This is a **learning project** I built to bridge my backend engineering experience into data engineering. It takes a simulated Walmart retail dataset (6 CSV files) through a complete Medallion Architecture pipeline — from raw ingestion to analytics-ready star schema — while solving real data engineering problems like duplicate handling, schema validation, and incremental loads.

I focused on **understanding how pipelines fail in real life** and designing safeguards against those failures, rather than just making data move from Point A to Point B.

---

## The Problem I Wanted to Solve

Raw retail data (customers, orders, products, stores, employees, order items) typically lands as CSV extracts. Without a structured pipeline, this data suffers from:

- **Duplicate records** when files are reprocessed
- **No change tracking** — you can't tell when a customer's address was updated
- **Silent data corruption** — missing partitions, null spikes, schema drift
- **No single source of truth** for BI dashboards
- **Manual, error-prone** reporting workflows

**My goal:** Build an automated pipeline that doesn't just transform data, but validates it at every stage and fails fast when something is wrong.

---

## Architecture
<img width="905" height="422" alt="Screenshot 2026-08-04 192540" src="https://github.com/user-attachments/assets/95fba43f-8201-4a85-849b-1b1368060685" />




```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   6 CSVs    │────▶│    Bronze    │────▶│    Silver    │────▶│     Gold     │
│  (Source)   │     │  PostgreSQL  │     │  Databricks  │     │  Databricks  │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                   │                    │                    │
       ▼                   ▼                    ▼                    ▼
  load_data.py      CDC / Ingestion     dbt Incremental      dbt Snapshots
  (psycopg2)        (Airflow Python)     Models + Tests       + Fact Tables
```

| Layer | Purpose | What Happens Here |
|-------|---------|-------------------|
| **Bronze** | Raw landing | CSVs loaded into PostgreSQL with SCD audit columns (`created_timestamp`, `updated_timestamp`, `is_active`) |
| **Silver — Technical** | Cleansed & deduplicated | dbt incremental models merge only changed rows using `updated_timestamp` watermarking |
| **Silver — Business** | Denormalized for BI | One Big Table (OBT) joining all 6 entities via a metadata-driven Jinja macro |
| **Gold** | Analytics-ready | SCD Type-2 snapshots for dimension history + star schema fact table |

---

## End-to-End Data Flow

### Phase 1: Bronze Ingestion
- `load_data.py` uses `psycopg2` and PostgreSQL `COPY ... FROM STDIN` for fast bulk loading
- 6 CSV files mapped to 6 raw tables with primary keys and audit timestamps

### Phase 2: CDC to Databricks
- Airflow task `ingest_cdc` uses the Databricks SDK (`WorkspaceClient`) to trigger and monitor an external ingestion job
- Polls job status with backoff until terminal state (success / failure / skipped)
- Data lands in Databricks Delta Lake Bronze tables

### Phase 3: Source Freshness Check
- `dbt source freshness` validates that Bronze data isn't stale before any transformation runs
- **First quality gate:** If source data is old, the DAG stops here

### Phase 4: Silver Technical (Incremental)
- `dbt run --select silver_t` builds 6 incremental models
- Each model uses `unique_key` + `updated_timestamp` to merge only new/changed records
- Adds `processed_at` timestamp for audit trail
- **Second quality gate:** `dbt test --select silver_t` enforces `not_null` and `unique` constraints on primary keys

### Phase 5: Silver Business (OBT)
- `dbt run --select silver_b` builds a denormalized One Big Table
- Uses a **metadata-driven Jinja macro**: table names, columns, aliases, and join conditions are stored in a config array
- Generates a dynamic 6-table LEFT JOIN query at compile time
- Column names are prefixed to prevent collisions (`customer_city` vs `store_city`)

### Phase 6: Gold Dimensions (SCD Type 2)
- `dbt snapshot` tracks historical changes on dimension tables
- Uses `strategy: timestamp` with `updated_at` column
- Automatically manages `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id`
- Current records use `'9999-12-31'` instead of NULL for cleaner BI queries

### Phase 7: Gold Facts
- `dbt run --select gold/fact` builds the fact table at `order_item` grain
- Joins ephemeral prep models to dimension snapshots

---

## Pipeline Orchestration (Airflow DAG)

```
ingest_cdc
    │
    ▼
clean_target ──▶ source_freshness
                    │
                    ▼
        silver_technical ──▶ silver_technical_tests
                                    │
                                    ▼
                        silver_business ──▶ silver_business_tests
                                                    │
                                                    ▼
                                    gold_ephemeral ──▶ gold_dimensions
                                                            │
                                                            ▼
                                                        gold_facts
```

**Why this dependency structure matters:**
- Tests are **blocking gates**, not afterthoughts. If Silver Technical tests fail, Silver Business never runs.
- This prevents bad data from ever reaching the Gold layer or BI dashboards.

---

## Key Technical Decisions

### 1. Incremental Processing Instead of Full Refresh
I used dbt's `materialized='incremental'` with `updated_timestamp` watermarking. On subsequent runs, only rows newer than the last `updated_timestamp` are processed. This means:
- Less compute waste on unchanged data
- Faster pipeline runs
- Lower risk of accidentally overwriting good data

### 2. Metadata-Driven OBT via Jinja
Instead of writing a static 80-line SQL query with 6 LEFT JOINs, I stored table metadata in a Jinja `configs` array. A macro loops through it to generate the SELECT and FROM/JOIN clauses. Adding a new source now requires ~5 lines of config instead of rewriting SQL.

### 3. SCD Type 2 via dbt Snapshots
Without dbt snapshots, I'd need to write manual MERGE logic: hash rows, detect changes, expire old records, insert new versions, handle overlapping date ranges. dbt snapshots automate this with a single config block per dimension table.

### 4. Docker Compose for Reproducibility
The `docker-compose.yaml` spins up a local Airflow cluster (Postgres metadata, Redis broker, CeleryExecutor workers, scheduler, webserver, triggerer). This means:
- The entire pipeline runs identically on any machine with Docker
- No "works on my laptop" issues
- Easy teardown with `docker-compose down`

---

## Data Quality & Reliability

This project implements a **defense-in-depth** approach to data quality:

| Layer | Check | How It's Implemented |
|-------|-------|---------------------|
| Source | Freshness | `dbt source freshness` — DAG fails if Bronze data is stale |
| Silver Technical | Primary Key Integrity | `not_null` + `unique` generic tests on `product_id`, `order_id` |
| Silver Business | Referential Logic | dbt tests block Gold if business rules break |
| Gold | Historical Accuracy | SCD2 snapshots prevent silent overwrites of dimension history |
| Orchestration | Failure Handling | Airflow retries, health checks, and task dependencies prevent partial pipeline runs |

**Real-world problems this guards against:**
- Source file arrives late → caught by `source_freshness`
- Duplicate records after rerun → caught by `unique` tests
- Schema drift in source → caught by dbt compilation errors
- Yesterday's data gets overwritten → prevented by SCD2 snapshots
- Pipeline fails halfway → Airflow's task boundaries prevent partial Gold updates

---

## Tech Stack

| Category | Tool | Role |
|----------|------|------|
| Orchestration | Apache Airflow 3.2 (CeleryExecutor) | DAG scheduling, retries, dependency management |
| Transformation | dbt-core + dbt-databricks | SQL models, tests, snapshots, documentation |
| Compute | Databricks Delta Lake | Distributed processing, ACID transactions |
| Source DB | PostgreSQL 16 | Bronze layer staging |
| Containerization | Docker + Docker Compose | Reproducible local environment |
| Data Loader | Python + psycopg2 | Bulk CSV ingestion |
| Version Control | Git + GitHub | CI-ready project structure |

---

## Project Structure

```
Walmart_Airflow_DBT_Project/
├── airflow_dbt_project/
│   ├── dags/
│   │   └── orchestrate.py          # 9-task Airflow DAG
│   ├── config/
│   │   └── airflow.cfg             # Airflow configuration
│   ├── walmart_project/            # dbt project root
│   │   ├── models/
│   │   │   ├── source/
│   │   │   │   └── sources.yml     # 6 source declarations
│   │   │   ├── silver_t/           # Incremental cleanse models
│   │   │   ├── silver_b/           # OBT (Jinja macro)
│   │   │   └── gold/               # Ephemeral prep + snapshots + facts
│   │   ├── snapshots/              # SCD Type-2 dimension tracking
│   │   ├── tests/                  # Custom business tests
│   │   ├── macros/                 # Reusable Jinja macros
│   │   ├── dbt_project.yml         # Model configs, schema routing
│   │   └── profiles.yml            # Databricks connection
│   ├── docker-compose.yaml         # 8-service Airflow cluster
│   ├── Dockerfile                  # Custom image (Airflow + dbt)
│   ├── requirements.txt            # Python dependencies
│   └── .env                        # Airflow UID config
│
├── walmart_dataset/
│   ├── data/                       # 6 CSV source files
│   ├── ddl/
│   │   └── walmart_schema.sql      # Bronze table DDL
│   └── load_data.py                # Idempotent bulk loader
│
└── README.md
```

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Databricks workspace (for Silver/Gold layers)
- Python 3.11+

### 1. Clone & Configure
```bash
git clone https://github.com/anshlambagit/Walmart_Airflow_DBT_Project.git
cd Walmart_Airflow_DBT_Project/airflow_dbt_project
```
Update `profiles.yml` and `orchestrate.py` with your Databricks credentials (use environment variables for tokens).

### 2. Start Airflow Cluster
```bash
docker-compose up --build -d
```
- Airflow UI: http://localhost:8080
- Postgres: localhost:5432

### 3. Load Bronze Data
```bash
cd ../walmart_dataset
python load_data.py
```

### 4. Trigger Pipeline
```bash
docker exec -it airflow_scheduler airflow dags trigger orchestrate
```

---

## What I Learned

- **Incremental models are a design decision, not a checkbox.** You need a reliable watermark column, a deterministic unique key, and a cold-start fallback. Getting this wrong means duplicate data or missing rows.
- **Data quality tests should fail the pipeline.** Running tests after the pipeline completes is too late. Wiring `dbt test` as blocking Airflow tasks ensures bad data never reaches downstream consumers.
- **Jinja macros are SQL generators.** For repetitive patterns (like multi-table JOINs), metadata-driven templates reduce boilerplate and prevent copy-paste errors.
- **Containerization matters for reproducibility.** Being able to run `docker-compose up` and have the entire stack work identically on any machine is a game-changer for collaboration and debugging.

---


---

