# GCP Deployment Runbook

> **First time?** Follow `README.md` to set up and deploy.
> This file is for ongoing operations after deployment is complete.

---

## Project Details

| Setting | Value |
|---|---|
| Project ID | `etl-pipeline-492723` |
| Region | `us-central1` |
| BigQuery Dataset | `etlpipeline_dw` |
| Cloud Run Service | `supermarket-etl` |
| Cloud Storage Bucket | `etl-pipeline-492723-etlpipeline-raw` |
| Cloud Scheduler Job | `supermarket-daily-run` (daily 8am UTC) |

---

## Deployed Components

| Component | Name | Status |
|---|---|---|
| Cloud Run | `supermarket-etl` | Active |
| Cloud Storage | `etl-pipeline-492723-etlpipeline-raw` | Active |
| BigQuery | `dim_branch`, `dim_product`, `fact_sales`, `vw_sales_report` | Active |
| Cloud Scheduler | `supermarket-daily-run` | Active |
| Secret Manager | `kaggle-api-token` | Active |
| Looker Studio    | Connected to `vw_sales_report`        | Active |
| Cloud Monitoring | Log-based alert on pipeline failure   | Active |

---

## Recommended Order After Every Code Change

1. Pull latest code
2. Fix any Git conflicts
3. Authenticate Cloud Shell
4. Run unit tests
5. Validate BigQuery access
6. Redeploy Cloud Run
7. Run smoke tests
8. Verify data and logs

---

## Step 1 — Pull Latest Code

```bash
git pull origin main
```

If Git blocks the pull because of an untracked `Dockerfile`:

```bash
cp Dockerfile ~/Dockerfile.backup
rm Dockerfile
git pull origin main
```

---

## Step 2 — Authenticate Cloud Shell

If you see `You do not currently have an active account selected`:

```bash
gcloud auth login
gcloud config set project etl-pipeline-492723
gcloud auth application-default login
```

Confirm authentication:

```bash
gcloud auth list
gcloud config get-value project
bq ls
```

Expected:
- Active account shown in `gcloud auth list`
- `gcloud config get-value project` returns `etl-pipeline-492723`
- `bq ls` succeeds without error

If multiple accounts are active:

```bash
gcloud config set account YOUR_EMAIL
```

---

## Step 3 — Run Unit Tests

```bash
pip install -r requirements/requirements-dev.txt
pytest tests/test_etl.py -v
```

Expected output:

```
tests/test_etl.py::test_dim_branch_row_count PASSED
tests/test_etl.py::test_dim_product_row_count PASSED
tests/test_etl.py::test_fact_sales_row_count PASSED
...
17 passed
```

All 17 tests must pass before redeploying.

---

## Step 4 — Validate BigQuery Access

Before redeploying confirm BigQuery access works:

```bash
bq query --nouse_legacy_sql \
'SELECT COUNT(*) AS row_count FROM `etl-pipeline-492723.etlpipeline_dw.fact_sales`'
```

Expected: query runs and returns `row_count = 1000`.

If this fails with an auth error fix authentication in Step 2 first.

---

## Step 5 — Redeploy Cloud Run

```bash
export PROJECT_ID=etl-pipeline-492723
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

---

## Step 6 — Trigger ETL

### Via Cloud Scheduler (recommended)

```bash
gcloud scheduler jobs run supermarket-daily-run \
  --location=us-central1
```

### Via curl (manual test)

```bash
export SERVICE_URL=$(gcloud run services describe supermarket-etl \
  --platform managed \
  --region us-central1 \
  --format 'value(status.url)')

curl -X POST ${SERVICE_URL}/run \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json"
```

Expected response:

```
ETL pipeline triggered!

```
---

## Step 7 — Run Smoke Tests and Load-Isolation Check

```bash
bash tests/smoke_test_gcp.sh etl-pipeline-492723
```

Expected output:
```
Running GCP smoke tests for project: etl-pipeline-492723

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

If you see `ERROR: You do not currently have an active account selected` fix authentication in Step 2 and retry.

### Verify partial-load isolation behavior

The ETL now isolates each BigQuery staging load in its own `try/except` block. If one staging load or merge step fails, the pipeline records success/failure in `output_data/load_summary.json`, skips Cloud Storage upload, and raises an error instead of continuing into an inconsistent final state.

```bash
gcloud logging read \
  "resource.type=cloud_run_revision AND resource.labels.service_name=supermarket-etl" \
  --limit=100 \
  --freshness=15m \
  --format="value(textPayload)"
```

Look for:
- `LOAD dim_branch_staging loaded successfully ...`
- `LOAD dim_product_staging loaded successfully ...`
- `LOAD fact_sales_staging loaded successfully ...`
- or a matching `FAILED` message for the specific table
- `LOAD Summary -> succeeded: [...], failed: [...]`
- `GCS upload skipped to avoid inconsistent state`

---

## Step 8 — Verify Data and Logs

### BigQuery row counts

```bash
bq query --nouse_legacy_sql \
'SELECT COUNT(*) FROM `etl-pipeline-492723.etlpipeline_dw.fact_sales`'

bq query --nouse_legacy_sql \
'SELECT COUNT(*) FROM `etl-pipeline-492723.etlpipeline_dw.dim_branch`'

bq query --nouse_legacy_sql \
'SELECT COUNT(*) FROM `etl-pipeline-492723.etlpipeline_dw.dim_product`'
```

Expected: `1000 / 3 / 6 rows`

### Check logs

```bash
gcloud logging read \
  "resource.type=cloud_run_revision AND resource.labels.service_name=supermarket-etl" \
  --limit=50 \
  --format="value(textPayload)" \
  --freshness=10m
```
If a load-isolation test is being performed, a failed run is expected to show one or more `FAILED` entries in the load summary. In that case:
- successful staging loads may still appear in logs,
- later merge steps may be skipped,
- Cloud Storage upload should be skipped,
- the run should end with `ETL pipeline failed: ...`.

Look for:
- `LOAD dim_branch_staging loaded successfully ...`
- `LOAD dim_product_staging loaded successfully ...`
- `LOAD fact_sales_staging loaded successfully ...`
- `LOAD dimension merges completed successfully`
- `LOAD fact_sales merge completed successfully`
- `LOAD Summary -> succeeded: [...], failed: [...]`
- `ETL complete with data quality checks`

---

## Looker Studio

- URL: lookerstudio.google.com
- Data source: `etl-pipeline-492723` → `etlpipeline_dw` → `vw_sales_report`

---

## Quick Recovery

### One table failed during staging load

This is expected behavior for load-isolation testing.
- Check Cloud Run logs for which staging table failed.
- Confirm `LOAD Summary -> succeeded: [...], failed: [...]` appears.
- Confirm no Cloud Storage upload occurred after the failure.
- Fix the failing table issue, redeploy, and rerun the scheduler job.

### No active account
```bash
gcloud auth login
gcloud config set project etl-pipeline-492723
gcloud auth application-default login
```

### Wrong account active
```bash
gcloud auth list
gcloud config set account YOUR_EMAIL
```

### git pull blocked by untracked Dockerfile
```bash
cp Dockerfile ~/Dockerfile.backup
rm Dockerfile
git pull origin main
```

### Smoke test returns empty row count
Fix authentication first then retry smoke tests.

### Alert email not arriving
```bash
# Verify alert policy is enabled
gcloud alpha monitoring policies list \
  --format="table(displayName, enabled)"

# Verify notification channel is active
gcloud beta monitoring channels list \
  --format="table(displayName, type, enabled)"

# Confirm failure log entry exists
gcloud logging read \
  "resource.type=cloud_run_revision AND textPayload:\"ETL pipeline failed\"" \
  --limit=5 \
  --freshness=30m \
  --format="value(textPayload)"
```

> If log entry exists but no email — allow up to 10 minutes.
> Cloud Monitoring can take longer on first trigger.

---

## Monitoring Setup (run once)

### Step 1 — Create email notification channel

```bash
gcloud beta monitoring channels create \
  --display-name="ETL Email Alert" \
  --type=email \
  --channel-labels=email_address=YOUR_EMAIL@gmail.com
```

### Step 2 — Get channel ID

```bash
gcloud beta monitoring channels list \
  --format="value(name, displayName)"
```

Copy the number at the end of the name — e.g. `1234567890`.

### Step 3 — Create alert policy file

```bash
cat > etl-alert-policy.json << 'EOF'
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
EOF
```

Replace `YOUR_CHANNEL_ID` with the number from Step 2.

### Step 4 — Apply the alert policy

```bash
gcloud alpha monitoring policies create \
  --policy-from-file=etl-alert-policy.json
```

### Step 5 — Verify

```bash
gcloud alpha monitoring policies list \
  --filter="displayName='ETL Pipeline Failure Alert'" \
  --format="table(displayName, enabled)"
```

Expected:

```
DISPLAY_NAME ENABLED
ETL Pipeline Failure Alert true
```


---

## Test Monitoring Alert

To verify the alert works — force a failure and confirm email arrives:

```bash
# 1 — Trigger manual ETL run
export SERVICE_URL=$(gcloud run services describe supermarket-etl \
  --platform managed \
  --region us-central1 \
  --format 'value(status.url)')

curl -X POST ${SERVICE_URL}/run \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)"

# 2 — Check logs for failure entry (wait 30 seconds first)
gcloud logging read \
  "resource.type=cloud_run_revision AND resource.labels.service_name=supermarket-etl AND textPayload:\"ETL pipeline failed\"" \
  --limit=5 \
  --freshness=10m \
  --format="value(textPayload)"

# 3 — Check your configured email inbox
# Email arrives within 2–5 minutes
```

---

## Update Kaggle Token

```bash
echo -n "YOUR_NEW_KAGGLE_TOKEN" | gcloud secrets versions add kaggle-api-token \
  --data-file=-
```

---

## Cost Monitoring

```bash
# View current month billing
gcloud billing accounts list

# View project spend
gcloud billing projects describe etl-pipeline-492723
```

Expected: well within $300 free trial credit at less than $5 over 90 days.