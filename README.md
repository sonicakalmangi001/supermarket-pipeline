# Supermarket Sales ETL Pipeline

A Python-based ETL pipeline that extracts supermarket sales data from Kaggle, transforms it into a star schema dimensional model, loads it into SQLite locally or BigQuery on GCP using a staging swap pattern that fully prevents partial loads, and generates an analytical report with SQL window functions.

---
## Version History

This project was built incrementally across three versions.
Full release history is available on the [Releases page](https://github.com/sonicakalmangi001/supermarket-pipeline/releases).

| Version | What was added |
|---|---|
| [v1.0.0](https://github.com/sonicakalmangi001/supermarket-pipeline/releases/tag/v1.0.0) | Initial ETL pipeline with pandas, SQLite, data quality checks, star schema, 17 unit tests |
| [v1.1.0](https://github.com/sonicakalmangi001/supermarket-pipeline/releases/tag/v1.1.0) | GCP Cloud Run deployment, BigQuery integration, Secret Manager, Cloud Scheduler |
| [v1.2.0](https://github.com/sonicakalmangi001/supermarket-pipeline/releases/tag/v1.2.0) | Staging swap pattern for partial load prevention, load_summary.json, Cloud Monitoring alerts, load_summary.json unit tests (19 total) |

---

## Project Structure

```
supermarket-pipeline/
├── src/ # All ETL source code
│ ├── etl.py # Unified ETL script (local + GCP Cloud Run)
│ ├── etl.ipynb # Jupyter notebook for exploration
│ └── spark_etl.py # PySpark ETL version
│
├── requirements/ # All dependency files
│ ├── requirements.txt # GCP production dependencies
│ ├── requirements-notebook.txt # Notebook dependencies
│ ├── requirements-spark.txt # Spark dependencies
│ └── requirements-dev.txt # Development and testing dependencies
│
├── architecture/ # All architecture documentation
│   └── cloud-architecture.png # GCP architecture diagram
│
├── sql/                # SQL scripts
│ ├── create_tables.sql # DDL for dimension, fact, and staging tables
│ └── report.sql        # Analytical report query
│
├── tests/ # All tests
│ ├── _init_.py
│ ├── test_etl.py # Unit tests (pytest)
│ └── smoke_test_gcp.sh # GCP post-deployment smoke tests
│
├── Dockerfile                    # Cloud Run container definition
├── .env.example                  # Required environment variables template
├── .gitignore                    # Git ignore rules
├── CHANGELOG.md                  # Project version history
├── gcp-setup.md                  # GCP operational runbook
└── README.md                     # Project documentation
```

---

## Schema Design

### Dimension Tables

- **dim_branch** — Branch code and city (3 rows)
- **dim_product** — Product line (6 rows)

### Fact Table

- **fact_sales** — Sales transactions (1000 rows)

### Staging Tables (Partial Load Prevention)

- **dim_branch_staging** — Staging target for dim_branch during ETL load
- **dim_product_staging** — Staging target for dim_product during ETL load
- **fact_sales_staging** — Staging target for fact_sales during ETL load

> The ETL loads all data into staging first. Production tables are only
> updated after all three staging loads succeed. If any staging load
> fails, production keeps its last good data untouched and GCS upload
> is skipped entirely.

### Report View

- **vw_sales_report** — Analytical report with joins and window functions

---

## Partial Load Prevention (Staging Swap Pattern)

A key reliability feature of this pipeline is the two-phase staging swap
that prevents BigQuery from ever being left in an inconsistent state.

### The Problem It Solves

Without staging, if `dim_branch` and `dim_product` load successfully but
`fact_sales` fails, BigQuery is left with mismatched tables — reports
fail or return wrong data with no way to recover the previous state.

### How It Works
```
Phase 1 — Load to staging (production untouched):
dim_branch → dim_branch_staging ✓
dim_product → dim_product_staging ✓
fact_sales → fact_sales_staging ✗ FAILS
↓
RuntimeError raised
Production tables NOT updated
Last good data preserved ✓

Phase 1 — All staging loads succeed:
dim_branch → dim_branch_staging ✓
dim_product → dim_product_staging ✓
fact_sales → fact_sales_staging ✓
↓
Phase 2 — Swap staging to production:
dim_branch_staging → dim_branch ✓
dim_product_staging → dim_product ✓
fact_sales_staging → fact_sales ✓
↓
Phase 3 — Upload CSVs to Cloud Storage ✓
```

### Load Summary

Every pipeline run writes `output_data/load_summary.json`.

On full success:
```json
{
  "succeeded": ["dim_branch", "dim_product", "fact_sales"],
  "failed": [],
  "total": 3
}
```

On partial failure:
```json
{
  "succeeded": ["dim_branch", "dim_product"],
  "failed": ["fact_sales"],
  "total": 3
}
```

---

## Three ETL Approaches

| File               | Approach                                       | Runs on          |
| ------------------ | ---------------------------------------------- | ---------------- |
| `src/etl.ipynb`    | Interactive exploration and development        | Local Jupyter    |
| `src/etl.py`       | Production deployment with data quality checks | GCP Cloud Run    |
| `src/spark_etl.py` | Distributed processing for large datasets      | Local / Dataproc |


### When to use Spark vs Pandas

- **Pandas** (`src/etl.py`): datasets under 1GB, local or Cloud Run
- **Spark** (`src/spark_etl.py`): datasets over 1GB, cluster processing

---

## Run Locally (Jupyter Notebook)

### Setup virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements/requirements-notebook.txt
```

### Register Jupyter kernel

```bash
python -m ipykernel install --user \
  --name=supermarket-venv \
  --display-name "Supermarket Pipeline"
```

### Open notebook

```bash
jupyter notebook src/etl.ipynb
```

---

## Run Locally (Python Script)

```bash
source venv/bin/activate
pip install -r requirements/requirements.txt
python src/etl.py
```

Output files saved to `output_data/`:

- `supermarket_dw.sqlite` — SQLite database with all tables
- `dim_branch.csv` — Branch dimension
- `dim_product.csv` — Product dimension
- `fact_sales.csv` — Sales fact table
- `report.csv` — Analytical report
- `data_quality_report.json` — Data quality metrics
- `data_quality_rejects.csv` — Rejected rows detail
- `load_summary.json` — Load stage success/failure summary

---

## Run Spark Version Locally

```bash
source venv/bin/activate
pip install -r requirements/requirements-spark.txt
python src/spark_etl.py
```

Output saved to `spark_output/`:

- `dim_branch/` — Parquet files
- `dim_product/` — Parquet files
- `fact_sales/` — Parquet files
- `report/` — Analytical report as CSV
- `data_quality_report.json` — Data quality metrics
- `data_quality_rejects/` — Rejected rows

---

## Run Unit Tests

Unit tests validate ETL logic, data quality rules, dimension builds
and SQLite loading locally without needing GCP.

### Install test dependencies

```bash
source venv/bin/activate
pip install -r requirements/requirements-dev.txt
```

### Run all unit tests

```bash
pytest tests/test_etl.py -v
```

### Expected output

```
tests/test_etl.py::test_dim_branch_row_count PASSED
tests/test_etl.py::test_dim_branch_has_required_columns PASSED
tests/test_etl.py::test_dim_product_row_count PASSED
tests/test_etl.py::test_fact_sales_row_count PASSED
tests/test_etl.py::test_fact_sales_no_null_branch_keys PASSED
tests/test_etl.py::test_fact_sales_no_null_product_keys PASSED
tests/test_etl.py::test_total_amount_positive PASSED
tests/test_etl.py::test_rating_range PASSED
tests/test_etl.py::test_quantity_positive PASSED
tests/test_etl.py::test_sqlite_tables_exist PASSED
tests/test_etl.py::test_sqlite_row_counts PASSED
tests/test_etl.py::test_no_null_invoice_ids PASSED
tests/test_etl.py::test_valid_branches PASSED
tests/test_etl.py::test_valid_cities PASSED
tests/test_etl.py::test_valid_customer_types PASSED
tests/test_etl.py::test_valid_genders PASSED
tests/test_etl.py::test_valid_payment_methods PASSED
tests/test_etl.py::test_load_summary_created PASSED
tests/test_etl.py::test_load_summary_no_failures PASSED 

19 passed
```

---

## GCP Smoke Tests

Smoke tests verify the full pipeline ran correctly in GCP by checking
BigQuery row counts and Cloud Storage file existence.
Run these in GCP Cloud Shell after every deployment or scheduled run.

### Run smoke tests (in Cloud Shell)

```bash
bash tests/smoke_test_gcp.sh YOUR_PROJECT_ID
```

### Expected output

```
Running GCP smoke tests for project: YOUR_PROJECT_ID

Checking fact_sales...
PASS fact_sales: 1000 rows
Checking dim_branch...
PASS dim_branch: 3 rows
Checking dim_product...
PASS dim_product: 6 rows
Checking Cloud Storage...
PASS Cloud Storage: raw CSV file exists

All GCP smoke tests passed!
```

### What smoke tests check


| Test                  | Expected  | Description                   |
| --------------------- | --------- | ----------------------------- |
| fact_sales row count  | 1000 rows | All transactions loaded       |
| dim_branch row count  | 3 rows    | All branches loaded           |
| dim_product row count | 6 rows    | All product lines loaded      |
| Cloud Storage file    | exists    | Raw CSV uploaded successfully |


---

## Testing Strategy


| Test Type       | Tool      | Where       | What it validates                                                             |
| --------------- | --------- | ----------- | ------------------------------------------------------------------------------|
| Unit tests | pytest | Local Mac | ETL logic, transforms, data quality rules, SQLite loading, load_summary.json validation |
| GCP smoke tests | bash + bq | Cloud Shell | BigQuery row counts, Cloud Storage file existence                             |


---

## Monitoring & Alerting

Pipeline failures are automatically detected and alerted using
**Cloud Monitoring log-based alerts** — no third party tools needed.

### How It Works

```
run_etl() catches any exception
↓
prints "ETL pipeline failed: ..."
↓
Cloud Run forwards stdout to Cloud Logging automatically
↓
Cloud Monitoring watches for "ETL pipeline failed" in logs
↓
Email alert sent to YOUR_EMAIL@gmail.com within 2–5 minutes
```

### What Is Monitored

| Event | How detected | Alert |
|---|---|---|
| Pipeline exception | Log match on `"ETL pipeline failed"` | Email |
| Partial load failure | `load_summary.json` — failed list | Email + log |
| Data quality issues | `data_quality_report.json` — metrics | Log only |

### Setup (run once in Cloud Shell)

```bash
# Step 1 — Create email notification channel
gcloud beta monitoring channels create \
  --display-name="ETL Email Alert" \
  --type=email \
  --channel-labels=email_address=YOUR_EMAIL@gmail.com

# Step 2 — Get channel ID
gcloud beta monitoring channels list \
  --format="value(name, displayName)"

# Step 3 — Create alert policy (replace YOUR_CHANNEL_ID)
gcloud alpha monitoring policies create \
  --policy-from-file=etl-alert-policy.json
```

### Alert Policy Config (`etl-alert-policy.json`)

```json
{
  "displayName": "ETL Pipeline Failure Alert",
  "conditions": [
    {
      "displayName": "ETL pipeline failed log match",
      "conditionMatchedLog": {
        "filter": "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"supermarket-etl\" AND textPayload:\"ETL pipeline failed\""
      }
    }
  ],
  "alertStrategy": {
    "notificationRateLimit": {
      "period": "300s"
    }
  },
  "combiner": "OR",
  "notificationChannels": [
    "projects/etl-pipeline-492723/notificationChannels/YOUR_CHANNEL_ID"
  ]
}
```

> Note: `etl-alert-policy.json` is listed in `.gitignore` because it
> contains your personal notification channel ID.

---

## Deploy to GCP

> For operational details, real project IDs, and day-to-day runbook see `gcp-setup.md`

### Prerequisites

- GCP account with free trial ($300 credit)
- GCP project created
- gcloud CLI available via Cloud Shell

### Step 1 — Set your project

```bash
gcloud config set project YOUR_PROJECT_ID
export PROJECT_ID=$(gcloud config get-value project)
```

### Step 2 — Enable required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

### Step 3 — Create Cloud Storage bucket

```bash
gsutil mb -l us-central1 gs://${PROJECT_ID}-etlpipeline-raw
```

### Step 4 — Create BigQuery dataset

```bash
bq mk --dataset --location=US etlpipeline_dw
```

Then run `sql/create_tables.sql` in BigQuery console.

> `create_tables.sql` creates both the **production tables** and the
> **staging tables** (`dim_branch_staging`, `dim_product_staging`,
> `fact_sales_staging`) required by the partial load prevention pattern.

### Step 5 — Store Kaggle token in Secret Manager

```bash
echo -n "YOUR_KAGGLE_TOKEN" | gcloud secrets create kaggle-api-token \
  --data-file=- \
  --replication-policy="automatic"
```

### Step 6 — Grant Cloud Run service account permissions

```bash
export PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} \
  --format="value(projectNumber)")

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Step 7 — Clone repo in Cloud Shell

```bash
git clone https://github.com/sonicakalmangi001/supermarket-pipeline.git
cd supermarket-pipeline
```

### Step 8 — Build and deploy to Cloud Run

```bash
export REGION=us-central1

gcloud builds submit --tag gcr.io/${PROJECT_ID}/supermarket-etl

gcloud run deploy supermarket-etl \
  --image gcr.io/${PROJECT_ID}/supermarket-etl \
  --platform managed \
  --region ${REGION} \
  --no-allow-unauthenticated \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID} \
  --memory 512Mi \
  --timeout 300
```

Expected output:

```
Service [supermarket-etl] revision has been deployed and is serving
traffic at: https://supermarket-etl-xxxx-uc.a.run.app
```

### Step 9 — Set up Cloud Scheduler

```bash
export SERVICE_URL=$(gcloud run services describe supermarket-etl \
  --platform managed \
  --region ${REGION} \
  --format 'value(status.url)')

export SA_EMAIL=$(gcloud iam service-accounts list \
  --format="value(email)" \
  --filter="displayName:Default" | head -1)

gcloud scheduler jobs create http supermarket-daily-run \
  --schedule="0 8 * * *" \
  --uri="${SERVICE_URL}/run" \
  --http-method=POST \
  --oidc-service-account-email="${SA_EMAIL}" \
  --location=${REGION}
```

### Step 10 — Create report view in BigQuery

Run `sql/report.sql` in BigQuery console to create `vw_sales_report`.

### Step 11 — Connect Looker Studio

1. Go to lookerstudio.google.com
2. Create new report
3. Add BigQuery as data source
4. Select project → `etlpipeline_dw` → `vw_sales_report`
5. Build dashboard charts

---

## How the ETL Pipeline Works

```
Cloud Scheduler (daily 8am UTC)
│
▼
Cloud Run (src/etl.py)
│
├──► Secret Manager
│ retrieves Kaggle API token
│
▼
EXTRACT
- Downloads dataset from Kaggle API
- Raw CSV uploaded to Cloud Storage
│
▼
DATA QUALITY CHECKS
- Required column validation
- Null checks (17 columns)
- Invalid value detection
(branch, city, gender, payment, customer type)
- Business rule validation
(tax, total, cogs, gross income)
- Rejected rows logged to CSV
│
▼
TRANSFORM (Star Schema)
- Standardizes column names
- Builds dim_branch (3 rows, surrogate keys)
- Builds dim_product (6 rows, surrogate keys)
- Builds fact_sales (1000 rows, joins dimensions)
│
▼
LOAD (Staging Swap — Partial Load Prevention)
- Phase 1: All tables loaded to staging (production untouched)
  - dim_branch  → dim_branch_staging
  - dim_product → dim_product_staging
  - fact_sales  → fact_sales_staging
- Phase 2: If all staging succeed → swap to production
  - dim_branch_staging  → dim_branch
  - dim_product_staging → dim_product
  - fact_sales_staging  → fact_sales
- Phase 3: Upload CSVs to Cloud Storage
- If any staging fails → RuntimeError, production preserved
│
▼
REPORT
- vw_sales_report SQL view
- RANK() OVER (PARTITION BY branch)
- SUM() OVER (PARTITION BY branch)
- Joins all 3 tables
│
▼
Looker Studio Dashboard
- Total sales by product line
- Gross income by branch
- Average rating by city
- Sales performance table
│
▼
MONITORING (Cloud Monitoring)
- Log-based alert watches for "ETL pipeline failed"
- Email alert sent to your configured email within 2–5 minutes
- Rate limited to 1 alert per 5 minutes
- No code changes needed — Cloud Run logs stdout automatically
```

---

## GCP Components


| Component       | Purpose                                                    |
| --------------- | ---------------------------------------------------------- |
| Cloud Scheduler | Triggers ETL automatically daily at 8am UTC                |
| Cloud Run       | Runs src/etl.py as containerized Flask service             |
| Secret Manager  | Stores Kaggle API token securely                           |
| Cloud Storage   | Stores raw CSV files before processing                     |
| BigQuery        | Hosts dim_branch, dim_product, fact_sales, vw_sales_report |
| Looker Studio      | Dashboard and report visualization                      |
| Cloud Monitoring   | Log-based alert emails on pipeline failure              |


---

## Cost Estimate


| Service           | Usage                          | Cost                   |
| ----------------- | ------------------------------ | ---------------------- |
| Cloud Run         | 1 run/day, ~2 min execution    | ~$0 (free tier)        |
| Cloud Storage     | < 1 MB raw CSV                 | ~$0 (5 GB free)        |
| BigQuery          | < 1 GB storage, < 1 TB queries | ~$0 (free tier)        |
| Secret Manager    | 1 secret, < 10k accesses/month | ~$0 (6 secrets free)   |
| Cloud Scheduler   | 1 job, 1 trigger/day           | ~$0 (3 jobs free)      |
| Looker Studio     | Connected to BigQuery          | $0 (free forever)      |
| Cloud Build       | 1 build per deployment         | ~$0 (120 min/day free) |
| Cloud Monitoring  | 1 alert policy, log-based      | ~$0 (free tier)        |
| **Total monthly** |                                | **~$0 of $300 credit** |


### Free tier breakdown


| Service         | Free tier limit                 | This project uses     |
| --------------- | ------------------------------- | --------------------- |
| Cloud Run       | 2M requests/month + 360k GB-sec | ~30 requests/month    |
| Cloud Storage   | 5 GB storage                    | < 1 MB                |
| BigQuery        | 10 GB storage + 1 TB queries    | < 10 MB               |
| Secret Manager  | 6 active secrets                | 1 secret              |
| Cloud Scheduler | 3 jobs free                     | 1 job                 |
| Cloud Build     | 120 min/day                     | ~2 min per deployment |


This project runs entirely within GCP free tier limits and will consume
less than $5 of the $300 free trial credit over 90 days.