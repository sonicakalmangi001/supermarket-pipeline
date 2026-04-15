#!/bin/bash
# GCP Smoke Tests - Run after each Cloud Run deployment
# Usage: bash tests/smoke_test_gcp.sh YOUR_PROJECT_ID

PROJECT_ID=${1:-"etl-pipeline-492723"}
DATASET="etlpipeline_dw"

echo "Running GCP smoke tests for project: $PROJECT_ID"

# Test 1 - fact_sales row count
echo "Checking fact_sales..."
FACT_COUNT=$(bq query --nouse_legacy_sql --format=csv \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.fact_sales\`" | tail -1)
if [ "$FACT_COUNT" -ge "1000" ]; then
  echo "PASS fact_sales: $FACT_COUNT rows"
else
  echo "FAIL fact_sales: expected >= 1000 rows, got $FACT_COUNT"
  exit 1
fi

# Test 2 - dim_branch row count
echo "Checking dim_branch..."
BRANCH_COUNT=$(bq query --nouse_legacy_sql --format=csv \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.dim_branch\`" | tail -1)
if [ "$BRANCH_COUNT" -eq "3" ]; then
  echo "PASS dim_branch: $BRANCH_COUNT rows"
else
  echo "FAIL dim_branch: expected 3 rows, got $BRANCH_COUNT"
  exit 1
fi

# Test 3 - dim_product row count
echo "Checking dim_product..."
PRODUCT_COUNT=$(bq query --nouse_legacy_sql --format=csv \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.dim_product\`" | tail -1)
if [ "$PRODUCT_COUNT" -eq "6" ]; then
  echo "PASS dim_product: $PRODUCT_COUNT rows"
else
  echo "FAIL dim_product: expected 6 rows, got $PRODUCT_COUNT"
  exit 1
fi

# Test 4 - Cloud Storage raw file exists
echo "Checking Cloud Storage..."
gsutil ls gs://$PROJECT_ID-etlpipeline-raw/supermarket_sales_raw.csv > /dev/null 2>&1
if [ $? -eq 0 ]; then
  echo "PASS Cloud Storage: raw CSV file exists"
else
  echo "FAIL Cloud Storage: raw CSV file not found"
  exit 1
fi

# Check load_summary.json exists in Cloud Storage
echo "Checking load_summary.json..."
if gsutil ls gs://etl-pipeline-492723-etlpipeline-raw/load_summary.json > /dev/null 2>&1; then
    echo "PASS load_summary.json exists in Cloud Storage"
else
    echo "FAIL load_summary.json not found"
    FAILED=1
fi

echo ""
echo "All GCP smoke tests passed!"
