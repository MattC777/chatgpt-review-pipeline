# ChatGPT Reviews Automated ELT Pipeline

## Overview
This project implements a fully automated **ELT pipeline** (Extract → Load → Transform) for **Google Play ChatGPT app reviews**.  
The pipeline runs daily using **GitHub Actions** and stores data in **Snowflake** for downstream analytics.

---

## Goals and Benefits

- **Goals**
  - Automatically scrape daily reviews from Google Play.
  - Load raw data into Snowflake staging schema.
  - Transform raw data into clean, analytics-ready tables and views.

- **Benefits**
  - **Freshness**: Daily updated data.
  - **Reliability**: Standardized schema with deduplication.
  - **Efficiency**: Analysts can query clean tables directly.

---

## Architecture

```text
          ┌───────────────────┐
          │ GitHub Actions CI │  (daily / manual trigger: It is daily automatic, but you can manually scrape reviews whenerver you like)
          └─────────┬─────────┘
                    │
          1 Extract │  scripts/scrape.py
                    ▼
          Raw CSV: data/raw_YYYY-MM-DD.csv
                    │
          2 Load    │  scripts/load_to_snowflake.py
                    ▼
     Snowflake STAGING.RAW_REVIEWS
                    │
          3 Transform │ run_transform.py + transform.sql
                    ▼
     Snowflake ANALYTICS.REVIEWS_CLEAN
                    │
                    └─► ANALYTICS.V_RECENT_7D (7-day view)
```
## Repository Structure 

```text
chatgpt-review-pipeline/
├─ .github/workflows/daily-pipeline.yml   # CI/CD pipeline config
├─ scripts/
│  ├─ scrape.py                           # Extract reviews
│  ├─ load_to_snowflake.py                # Load to Snowflake staging
│  └─ run_transform.py                    # Run SQL transforms
├─ sql/
│  └─ transform.sql                       # Transformation logic
├─ requirements.txt                       # Python dependencies
└─ .gitignore                             # Ignore local/temp files
```

## Implementation Details

### 1. Extract — `scripts/scrape.py`
- Uses **`google-play-scraper`** to fetch reviews.  
- Saves results as `data/raw_YYYY-MM-DD.csv`.  
- Fetches only the last day’s reviews.  

### 2. Load — `scripts/load_to_snowflake.py`
- Reads the latest CSV.  
- Loads into **`STAGING.RAW_REVIEWS`** via `snowflake-connector-python`.  
- Adds `_INGESTED_AT` timestamp for audit.  

### 3. Transform — `scripts/run_transform.py` + `sql/transform.sql`
- Executes SQL inside Snowflake:  
  - **`ANALYTICS.REVIEWS_CLEAN`** → deduplicated, cleaned table.  
  - **`ANALYTICS.V_RECENT_7D`** → rolling 7-day view.  

---

## Snowflake Output

- **Staging Layer**  
  - `STAGING.RAW_REVIEWS`: raw scraped data.  

- **Analytics Layer**  
  - `ANALYTICS.REVIEWS_CLEAN`: cleaned, standardized data.  
  - `ANALYTICS.V_RECENT_7D`: last 7 days of cleaned reviews.  

---

## Scheduling (GitHub Actions)

- **Tool**: GitHub Actions (`.github/workflows/daily-pipeline.yml`)  
- **Triggers**:
  - Cron: `0 11 * * *` → daily at **11:00 UTC** (7:00 A.M. New York)  
  - Manual: `workflow_dispatch`  

**Pipeline steps**:
1. Checkout repo.  
2. Setup Python 3.12.  
3. Install dependencies (`requirements.txt`).  
4. Run `scrape.py` (extract).  
5. Run `load_to_snowflake.py` (load).  
6. Run `run_transform.py` (transform).  
7. Upload CSV artifact (optional).  

---

## Security and Secrets

All credentials are stored as **GitHub Secrets** (not in repo):

- `SNOWFLAKE_USER`  
- `SNOWFLAKE_PASSWORD`  
- `SNOWFLAKE_ACCOUNT`  
- `SNOWFLAKE_ROLE`  
- `SNOWFLAKE_WAREHOUSE`  
- `SNOWFLAKE_DATABASE`  
- `SNOWFLAKE_SCHEMA`  

---

## Validation and Monitoring

- **Verification queries in Snowflake**:
  ```sql
  USE DATABASE REVIEWS;

  SELECT COUNT(*) FROM STAGING.RAW_REVIEWS;
  SELECT COUNT(*) FROM ANALYTICS.REVIEWS_CLEAN;
  SELECT * FROM ANALYTICS.V_RECENT_7D LIMIT 10;


