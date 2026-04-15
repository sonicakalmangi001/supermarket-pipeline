import os
import json
import threading
import sqlite3
from pathlib import Path

import pandas as pd
import kagglehub
from flask import Flask
import requests

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.2f}'.format)

app = Flask(__name__)

# ── Config ─────────────────────────────────────────────────────────
DATASET_ID = "lovishbansal123/sales-of-a-supermarket"
OUTPUT_DIR = Path("./output_data")
OUTPUT_DIR.mkdir(exist_ok=True)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "etl-pipeline-492723")
DATASET    = "etlpipeline_dw"
BUCKET     = "etl-pipeline-492723-etlpipeline-raw"

# Detect environment
IS_GCP = os.environ.get("GCP_PROJECT_ID") is not None
print(f"Running in {'GCP' if IS_GCP else 'LOCAL'} mode")

# GCP imports only when running on GCP
if IS_GCP:
    from google.cloud import bigquery, storage, secretmanager

# ── Validation Constants ───────────────────────────────────────────
REQUIRED_COLUMNS = [
    "Invoice ID", "Branch", "City", "Customer type", "Gender", "Product line",
    "Unit price", "Quantity", "Tax 5%", "Total", "Date", "Time", "Payment",
    "cogs", "gross margin percentage", "gross income", "Rating"
]

ALLOWED_BRANCHES       = {"A", "B", "C"}
ALLOWED_CITIES         = {"Yangon", "Mandalay", "Naypyitaw"}
ALLOWED_CUSTOMER_TYPES = {"Member", "Normal"}
ALLOWED_GENDERS        = {"Male", "Female"}
ALLOWED_PAYMENTS       = {"Cash", "Credit card", "Ewallet"}
RATING_MIN, RATING_MAX = 0.0, 10.0
QUANTITY_MIN           = 1
PRICE_MIN              = 0.0
TOLERANCE              = 0.02

# ── Step 1: Standardize Columns ────────────────────────────────────
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames raw Kaggle dataset columns into a standardized snake_case format.

    Args:
        df (pd.DataFrame): The raw dataframe with original supermarket headers.

    Returns:
        pd.DataFrame: A dataframe with renamed, database-friendly columns.
    """
    rename_map = {
        "Tax 5%":                  "tax_amount",
        "Total":                   "total_amount",
        "Date":                    "sale_date",
        "Time":                    "sale_time",
        "Payment":                 "payment_method",
        "Invoice ID":              "invoice_id",
        "Branch":                  "branch",
        "City":                    "city",
        "Customer type":           "customer_type",
        "Gender":                  "gender",
        "Product line":            "product_line",
        "Unit price":              "unit_price",
        "Quantity":                "quantity",
        "cogs":                    "cogs",
        "gross margin percentage": "gross_margin_pct",
        "gross income":            "gross_income",
        "Rating":                  "rating",
    }
    return df.rename(columns=rename_map)

# ── Step 2: Data Quality Checks ────────────────────────────────────
def run_quality_checks(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Performs comprehensive data validation including schema checks, 
    categorical membership, numeric ranges, and financial calculation integrity.

    Logic:
    1. Validates presence of all required columns.
    2. Checks for nulls in critical fields (IDs, Prices, Dates).
    3. Validates business rules (e.g., Rating must be 0-10).
    4. Recalculates Tax, Total, and COGS to ensure mathematical consistency.
    5. Flags rows for rejection if they fail critical checks.

    Args:
        df_raw (pd.DataFrame): The raw input data.

    Returns:
        tuple: (clean_df, report_dict)
            - clean_df: Dataframe containing only valid, deduplicated records.
            - report_dict: Summary metrics of the validation process.
    """
    report = {"errors": [], "warnings": [], "metrics": {}}

    missing_columns = [c for c in REQUIRED_COLUMNS if c not in df_raw.columns]
    extra_columns   = [c for c in df_raw.columns if c not in REQUIRED_COLUMNS]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    if extra_columns:
        report["warnings"].append({"type": "extra_columns", "columns": extra_columns})

    report["metrics"]["raw_row_count"]         = int(len(df_raw))
    report["metrics"]["raw_duplicate_rows"]    = int(df_raw.duplicated().sum())
    report["metrics"]["duplicate_invoice_ids"] = int(df_raw["Invoice ID"].duplicated().sum())

    null_counts = df_raw[REQUIRED_COLUMNS].isna().sum().to_dict()
    report["metrics"]["null_counts"] = {k: int(v) for k, v in null_counts.items()}

    critical_null_cols = [
        "Invoice ID", "Branch", "City", "Product line", "Unit price",
        "Quantity", "Total", "Date", "Time"
    ]
    critical_null_rows = df_raw[critical_null_cols].isna().any(axis=1)
    report["metrics"]["critical_null_rows"] = int(critical_null_rows.sum())

    df = df_raw.copy()
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")
    df["Time"] = pd.to_datetime(df["Time"], format="%H:%M", errors="coerce").dt.time

    numeric_cols = ["Unit price", "Quantity", "Tax 5%", "Total", "cogs", "gross income", "Rating"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    invalid_date_rows = df["Date"].isna()
    invalid_time_rows = df["Time"].isna()
    report["metrics"]["invalid_dates"] = int(invalid_date_rows.sum())
    report["metrics"]["invalid_times"] = int(invalid_time_rows.sum())

    invalid_branch        = ~df["Branch"].isin(ALLOWED_BRANCHES)
    invalid_city          = ~df["City"].isin(ALLOWED_CITIES)
    invalid_customer_type = ~df["Customer type"].isin(ALLOWED_CUSTOMER_TYPES)
    invalid_gender        = ~df["Gender"].isin(ALLOWED_GENDERS)
    invalid_payment       = ~df["Payment"].isin(ALLOWED_PAYMENTS)

    report["metrics"]["invalid_branch_values"]        = int(invalid_branch.sum())
    report["metrics"]["invalid_city_values"]          = int(invalid_city.sum())
    report["metrics"]["invalid_customer_type_values"] = int(invalid_customer_type.sum())
    report["metrics"]["invalid_gender_values"]        = int(invalid_gender.sum())
    report["metrics"]["invalid_payment_values"]       = int(invalid_payment.sum())

    invalid_numeric = (
        (df["Unit price"] <= PRICE_MIN) |
        (df["Quantity"] < QUANTITY_MIN) |
        (df["Rating"] < RATING_MIN) |
        (df["Rating"] > RATING_MAX)
    )
    report["metrics"]["invalid_numeric_rows"] = int(invalid_numeric.fillna(True).sum())

    calc_tax    = (df["cogs"] * 0.05).round(2)
    calc_total  = (df["cogs"] + df["Tax 5%"].fillna(0)).round(2)
    calc_line   = (df["Unit price"] * df["Quantity"]).round(2)

    tax_mismatch          = (df["Tax 5%"] - calc_tax).abs() > TOLERANCE
    total_mismatch        = (df["Total"] - calc_total).abs() > TOLERANCE
    cogs_mismatch         = (df["cogs"] - calc_line).abs() > TOLERANCE
    gross_income_mismatch = (df["gross income"] - df["Tax 5%"].fillna(0)).abs() > TOLERANCE

    report["metrics"]["tax_mismatch_rows"]          = int(tax_mismatch.fillna(False).sum())
    report["metrics"]["total_mismatch_rows"]        = int(total_mismatch.fillna(False).sum())
    report["metrics"]["cogs_mismatch_rows"]         = int(cogs_mismatch.fillna(False).sum())
    report["metrics"]["gross_income_mismatch_rows"] = int(gross_income_mismatch.fillna(False).sum())

    reject_mask = (
        critical_null_rows    |
        invalid_date_rows     |
        invalid_time_rows     |
        invalid_branch        |
        invalid_city          |
        invalid_customer_type |
        invalid_gender        |
        invalid_payment       |
        invalid_numeric.fillna(True)
    )

    report["metrics"]["rejected_rows"] = int(reject_mask.sum())
    report["metrics"]["clean_rows"]    = int((~reject_mask).sum())

    warn_checks = {
        "duplicate_rows":             report["metrics"]["raw_duplicate_rows"],
        "duplicate_invoice_ids":      report["metrics"]["duplicate_invoice_ids"],
        "tax_mismatch_rows":          report["metrics"]["tax_mismatch_rows"],
        "total_mismatch_rows":        report["metrics"]["total_mismatch_rows"],
        "cogs_mismatch_rows":         report["metrics"]["cogs_mismatch_rows"],
        "gross_income_mismatch_rows": report["metrics"]["gross_income_mismatch_rows"],
    }
    for check_name, count in warn_checks.items():
        if count > 0:
            report["warnings"].append({"type": check_name, "count": int(count)})

    clean_df = df.loc[~reject_mask].copy()
    clean_df = clean_df.drop_duplicates(subset=["Invoice ID"], keep="first")
    report["metrics"]["clean_rows_after_invoice_dedup"] = int(len(clean_df))

    reject_reasons = pd.DataFrame({
        "invoice_id":            df_raw["Invoice ID"],
        "critical_null":         critical_null_rows,
        "invalid_date":          invalid_date_rows,
        "invalid_time":          invalid_time_rows,
        "invalid_branch":        invalid_branch,
        "invalid_city":          invalid_city,
        "invalid_customer_type": invalid_customer_type,
        "invalid_gender":        invalid_gender,
        "invalid_payment":       invalid_payment,
        "invalid_numeric":       invalid_numeric.fillna(True),
        "tax_mismatch":          tax_mismatch.fillna(False),
        "total_mismatch":        total_mismatch.fillna(False),
        "cogs_mismatch":         cogs_mismatch.fillna(False),
        "gross_income_mismatch": gross_income_mismatch.fillna(False),
        "rejected":              reject_mask,
    })
    reject_reasons.to_csv(OUTPUT_DIR / "data_quality_rejects.csv", index=False)

    with open(OUTPUT_DIR / "data_quality_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    return clean_df, report

# ── Step 3: Extract ────────────────────────────────────────────────
def extract_data() -> pd.DataFrame:
    """
    Downloads the supermarket dataset from Kaggle Hub. 
    In GCP environments, it fetches the Kaggle API token from Secret Manager first.

    Returns:
        pd.DataFrame: The raw supermarket sales data.

    Raises:
        FileNotFoundError: If no CSV is found in the downloaded Kaggle bundle.
    """
    if IS_GCP:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/kaggle-api-token/versions/latest"
        response = client.access_secret_version(request={"name": name})
        os.environ["KAGGLE_API_TOKEN"] = response.payload.data.decode("UTF-8")

    dataset_dir = kagglehub.dataset_download(DATASET_ID)
    csv_files = []
    for root, _, files in os.walk(dataset_dir):
        for file in files:
            if file.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    if not csv_files:
        raise FileNotFoundError("No CSV file found in downloaded dataset")
    
    return pd.read_csv(csv_files[0])

# ── Step 4: Transform ──────────────────────────────────────────────
def transform_data(df_raw: pd.DataFrame):
    """
    Transforms raw data into a Star Schema (Fact and Dimension tables).

    Operations:
    1. Runs Data Quality checks.
    2. Standardizes column names.
    3. Extracts `dim_branch` (Branch and City mapping).
    4. Extracts `dim_product` (Unique product lines).
    5. Creates `fact_sales` by mapping dimension keys and filtering columns.

    Args:
        df_raw (pd.DataFrame): The raw data from the extraction phase.

    Returns:
        tuple: (dim_branch, dim_product, fact_sales, dq_report)
    """
    df_clean, dq_report = run_quality_checks(df_raw)
    df = standardize_columns(df_clean)
    df["sale_date"] = pd.to_datetime(df["sale_date"])

    dim_branch = (
        df[["branch", "city"]]
        .drop_duplicates()
        .sort_values(["branch", "city"])
        .reset_index(drop=True)
        .rename(columns={"branch": "branch_code"})
    )
    dim_branch["branch_key"] = range(1, len(dim_branch) + 1)
    dim_branch = dim_branch[["branch_key", "branch_code", "city"]]

    dim_product = (
        df[["product_line"]]
        .drop_duplicates()
        .sort_values(["product_line"])
        .reset_index(drop=True)
    )
    dim_product["product_key"] = range(1, len(dim_product) + 1)
    dim_product = dim_product[["product_key", "product_line"]]

    fact_sales = (
        df.merge(dim_branch.rename(columns={"branch_code": "branch"}), on=["branch", "city"], how="left")
        .merge(dim_product, on="product_line", how="left")
    )
    fact_sales = fact_sales[[
        "invoice_id", "branch_key", "product_key", "sale_date", "sale_time",
        "customer_type", "gender", "payment_method", "unit_price", "quantity",
        "tax_amount", "total_amount", "cogs", "gross_margin_pct",
        "gross_income", "rating"
    ]].copy()
    fact_sales.insert(0, "sales_key", range(1, len(fact_sales) + 1))

    return dim_branch, dim_product, fact_sales, dq_report

# ── Step 5: Load ───────────────────────────────────────────────────
def load_data(
    dim_branch: pd.DataFrame,
    dim_product: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> None:
    """Route transformed tables to the appropriate storage backend.

    In GCP mode, loads each table to BigQuery staging, merges into final
    warehouse tables, then uploads CSV copies to Cloud Storage only if all
    BigQuery steps succeeded. In local mode, writes all tables to SQLite.

    Args:
        dim_branch: Branch dimension table.
        dim_product: Product dimension table.
        fact_sales: Fact sales table.

    Raises:
        RuntimeError: If one or more staging or merge steps fail.
    """
    if IS_GCP:
        succeeded = []
        failed = []

        try:
            _load_to_bigquery_staging(dim_branch, "dim_branch")
            succeeded.append("dim_branch_staging")
            print(f"[LOAD] ✓ dim_branch_staging loaded successfully ({len(dim_branch)} rows)")
        except Exception as e:
            failed.append("dim_branch_staging")
            print(f"[LOAD] ✗ dim_branch_staging FAILED: {e}")

        try:
            _load_to_bigquery_staging(dim_product, "dim_product")
            succeeded.append("dim_product_staging")
            print(f"[LOAD] ✓ dim_product_staging loaded successfully ({len(dim_product)} rows)")
        except Exception as e:
            failed.append("dim_product_staging")
            print(f"[LOAD] ✗ dim_product_staging FAILED: {e}")

        try:
            _load_to_bigquery_staging(fact_sales, "fact_sales")
            succeeded.append("fact_sales_staging")
            print(f"[LOAD] ✓ fact_sales_staging loaded successfully ({len(fact_sales)} rows)")
        except Exception as e:
            failed.append("fact_sales_staging")
            print(f"[LOAD] ✗ fact_sales_staging FAILED: {e}")

        if not failed:
            try:
                _merge_dim_branch_and_product()
                succeeded.extend(["dim_branch_merge", "dim_product_merge"])
                print("[LOAD] ✓ dimension merges completed successfully")
            except Exception as e:
                failed.append("dimension_merges")
                print(f"[LOAD] ✗ dimension_merges FAILED: {e}")

        if not failed:
            try:
                _merge_fact_sales()
                succeeded.append("fact_sales_merge")
                print("[LOAD] ✓ fact_sales merge completed successfully")
            except Exception as e:
                failed.append("fact_sales_merge")
                print(f"[LOAD] ✗ fact_sales_merge FAILED: {e}")

        load_summary = {
            "succeeded": succeeded,
            "failed": failed,
            "total": 6,
        }
        with open(OUTPUT_DIR / "load_summary.json", "w") as f:
            json.dump(load_summary, f, indent=2)

        print(f"[LOAD] Summary — succeeded: {succeeded}, failed: {failed}")

        if failed:
            raise RuntimeError(
                f"Load stage incomplete — "
                f"succeeded: {succeeded}, "
                f"failed: {failed}. "
                f"GCS upload skipped to avoid inconsistent state."
            )

        _upload_to_gcs(dim_branch, dim_product, fact_sales)

    else:
        _load_to_sqlite(dim_branch, dim_product, fact_sales)

        load_summary = {
            "succeeded": ["dim_branch", "dim_product", "fact_sales"],
            "failed": [],
            "total": 3,
        }
        with open(OUTPUT_DIR / "load_summary.json", "w") as f:
            json.dump(load_summary, f, indent=2)

def _merge_fact_sales() -> None:
    """
    Merge fact_sales_staging into fact_sales in BigQuery.
    Uses invoice_id as the transaction-level business key.
    """
    client = bigquery.Client(project=PROJECT_ID)

    fact_merge_sql = f"""
    MERGE `{PROJECT_ID}.{DATASET}.fact_sales` T
    USING `{PROJECT_ID}.{DATASET}.fact_sales_staging` S
    ON T.invoice_id = S.invoice_id
    WHEN MATCHED THEN
      UPDATE SET
        T.branch_key = S.branch_key,
        T.product_key = S.product_key,
        T.sale_date = S.sale_date,
        T.sale_time = S.sale_time,
        T.customer_type = S.customer_type,
        T.gender = S.gender,
        T.payment_method = S.payment_method,
        T.unit_price = S.unit_price,
        T.quantity = S.quantity,
        T.tax_amount = S.tax_amount,
        T.total_amount = S.total_amount,
        T.cogs = S.cogs,
        T.gross_margin_pct = S.gross_margin_pct,
        T.gross_income = S.gross_income,
        T.rating = S.rating
    WHEN NOT MATCHED THEN
      INSERT (
        sales_key,
        invoice_id,
        branch_key,
        product_key,
        sale_date,
        sale_time,
        customer_type,
        gender,
        payment_method,
        unit_price,
        quantity,
        tax_amount,
        total_amount,
        cogs,
        gross_margin_pct,
        gross_income,
        rating
      )
      VALUES (
        S.sales_key,
        S.invoice_id,
        S.branch_key,
        S.product_key,
        S.sale_date,
        S.sale_time,
        S.customer_type,
        S.gender,
        S.payment_method,
        S.unit_price,
        S.quantity,
        S.tax_amount,
        S.total_amount,
        S.cogs,
        S.gross_margin_pct,
        S.gross_income,
        S.rating
      )
    """

    client.query(fact_merge_sql).result()
    print("Merged fact_sales_staging into fact_sales")

def _upload_to_gcs(dim_branch, dim_product, fact_sales):
    """Uploads dimension and fact tables as CSVs to a Google Cloud Storage bucket."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET)
    for name, df in [
        ("dim_branch",  dim_branch),
        ("dim_product", dim_product),
        ("fact_sales",  fact_sales)
    ]:
        blob = bucket.blob(f"{name}.csv")
        blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
        print(f"Uploaded {name}.csv to gs://{BUCKET}/")

def _load_to_sqlite(dim_branch, dim_product, fact_sales):
    """
    Creates a local SQLite database with proper schema definitions and 
    Foreign Key relationships, then populates it.
    """
    db_path = OUTPUT_DIR / "supermarket_dw.sqlite"
    conn = sqlite3.connect(db_path)

    conn.execute("DROP TABLE IF EXISTS fact_sales")
    conn.execute("DROP TABLE IF EXISTS dim_branch")
    conn.execute("DROP TABLE IF EXISTS dim_product")

    conn.execute("""
    CREATE TABLE dim_branch (
    branch_key INTEGER PRIMARY KEY,
    branch_code TEXT,
    city TEXT
    )""")

    conn.execute("""
    CREATE TABLE dim_product (
        product_key INTEGER PRIMARY KEY,
        product_line TEXT UNIQUE
    )""")

    conn.execute("""
    CREATE TABLE fact_sales (
        sales_key INTEGER PRIMARY KEY,
        invoice_id TEXT,
        branch_key INTEGER,
        product_key INTEGER,
        sale_date TEXT,
        sale_time TEXT,
        customer_type TEXT,
        gender TEXT,
        payment_method TEXT,
        unit_price REAL,
        quantity INTEGER,
        tax_amount REAL,
        total_amount REAL,
        cogs REAL,
        gross_margin_pct REAL,
        gross_income REAL,
        rating REAL,
        FOREIGN KEY (branch_key) REFERENCES dim_branch(branch_key),
        FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
    )""")

    dim_branch.to_sql("dim_branch",   conn, if_exists="append", index=False)
    dim_product.to_sql("dim_product", conn, if_exists="append", index=False)
    fact_sales.to_sql("fact_sales",   conn, if_exists="append", index=False)
    conn.close()
    print(f"SQLite database saved to {db_path}")

def _load_to_bigquery_staging(df: pd.DataFrame, table_name: str) -> None:
    """
    Load a dataframe into a BigQuery staging table.

    Args:
        df: DataFrame to load.
        table_name: Base table name, such as dim_branch, dim_product, or fact_sales.
    """
    client = bigquery.Client(project=PROJECT_ID)
    staging_table_id = f"{PROJECT_ID}.{DATASET}.{table_name}_staging"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows into staging table {staging_table_id}")


def _merge_dim_branch_and_product() -> None:
    """
    Merge dimension staging tables into dimension target tables in BigQuery.
    Inserts new rows and updates existing rows based on business keys.
    """
    client = bigquery.Client(project=PROJECT_ID)

    dim_branch_merge_sql = f"""
    MERGE `{PROJECT_ID}.{DATASET}.dim_branch` T
    USING `{PROJECT_ID}.{DATASET}.dim_branch_staging` S
    ON T.branch_code = S.branch_code
    WHEN MATCHED THEN
      UPDATE SET
        T.city = S.city
    WHEN NOT MATCHED THEN
      INSERT (branch_key, branch_code, city)
      VALUES (S.branch_key, S.branch_code, S.city)
    """

    dim_product_merge_sql = f"""
    MERGE `{PROJECT_ID}.{DATASET}.dim_product` T
    USING `{PROJECT_ID}.{DATASET}.dim_product_staging` S
    ON T.product_line = S.product_line
    WHEN MATCHED THEN
      UPDATE SET
        T.product_line = S.product_line
    WHEN NOT MATCHED THEN
      INSERT (product_key, product_line)
      VALUES (S.product_key, S.product_line)
    """

    client.query(dim_branch_merge_sql).result()
    print("Merged dim_branch_staging into dim_branch")

    client.query(dim_product_merge_sql).result()
    print("Merged dim_product_staging into dim_product")

# ── Step 6: Report ─────────────────────────────────────────────────
def generate_report():
    """
    Executes a SQL aggregation query to generate a business performance report.
    Calculates units sold, total sales, and sales ranking within branches.

    Returns:
        pd.DataFrame: The resulting analytical report.
    """
    query = """
    SELECT
        b.branch_code,
        b.city,
        p.product_line,
        COUNT(f.sales_key) AS transaction_count,
        SUM(f.quantity) AS total_units_sold,
        ROUND(SUM(f.total_amount), 2) AS total_sales,
        ROUND(SUM(f.gross_income), 2) AS total_gross_income,
        ROUND(AVG(f.rating), 2) AS avg_rating,
        RANK() OVER (
            PARTITION BY b.branch_code
            ORDER BY SUM(f.total_amount) DESC
        ) AS sales_rank_within_branch,
        ROUND(
            100.0 * SUM(f.total_amount) /
            SUM(SUM(f.total_amount)) OVER (PARTITION BY b.branch_code),
            2
        ) AS pct_of_branch_sales
    FROM fact_sales f
    JOIN dim_branch b ON f.branch_key = b.branch_key
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY b.branch_code, b.city, p.product_line
    ORDER BY b.branch_code, sales_rank_within_branch, p.product_line
    """

    if IS_GCP:
        bq_query = query \
            .replace("dim_branch",  f"`{PROJECT_ID}.{DATASET}.dim_branch`") \
            .replace("dim_product", f"`{PROJECT_ID}.{DATASET}.dim_product`") \
            .replace("fact_sales",  f"`{PROJECT_ID}.{DATASET}.fact_sales`")
        client    = bigquery.Client(project=PROJECT_ID)
        report_df = client.query(bq_query).to_dataframe()
    else:
        db_path   = OUTPUT_DIR / "supermarket_dw.sqlite"
        conn      = sqlite3.connect(db_path)
        report_df = pd.read_sql_query(query, conn)
        conn.close()

    report_df.to_csv(OUTPUT_DIR / "report.csv", index=False)
    print(report_df.to_string())
    return report_df

# ── Main ETL ───────────────────────────────────────────────────────
def run_etl():
    """
    The main entry point for the ETL process. 
    Sequentially executes Extract, Transform, and Load steps, 
    then triggers the final analytical report.
    """
    try:
        df_raw = extract_data()
        dim_branch, dim_product, fact_sales, dq_report = transform_data(df_raw)

        # Save CSVs locally always
        dim_branch.to_csv(OUTPUT_DIR  / "dim_branch.csv",  index=False)
        dim_product.to_csv(OUTPUT_DIR / "dim_product.csv", index=False)
        fact_sales.to_csv(OUTPUT_DIR  / "fact_sales.csv",  index=False)

        load_data(dim_branch, dim_product, fact_sales)
        report_df = generate_report()

        print("ETL complete with data quality checks")
        print(json.dumps(dq_report["metrics"], indent=2, default=str))
        print(report_df.head().to_string())

    except Exception as e:
        error_msg = f"ETL pipeline failed in project {PROJECT_ID}: {str(e)}"
        print(error_msg)
        raise

# ── Flask Routes ───────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    """Simple health check endpoint."""
    return "ETL Service is running!", 200

@app.route("/run", methods=["POST"])
def trigger_etl():
    """
    Endpoint to trigger the ETL process asynchronously via a thread.
    Useful for triggering via a webhook or Cloud Scheduler.
    """
    thread = threading.Thread(target=run_etl)
    thread.start()
    return "ETL pipeline triggered!", 200

# ── Entry Point ────────────────────────────────────────────────────
if __name__ == "__main__":
    if IS_GCP:
        port = int(os.environ.get("PORT", 8080))
        app.run(host="0.0.0.0", port=port)
    else:
        run_etl()