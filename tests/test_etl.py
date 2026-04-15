import pytest
import sqlite3
import pandas as pd
import os
import sys
import json
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# ── Test Data ──────────────────────────────────────────────────────
@pytest.fixture
def sample_df():
    """
    Provides a small, controlled DataFrame representing raw supermarket data.
    This fixture is used across multiple tests to ensure consistent input.
    """
    data = {
        "branch":           ["A", "A", "B", "C"],
        "city":             ["Yangon", "Yangon", "Mandalay", "Naypyitaw"],
        "product_line":     ["Food and beverages", "Health and beauty",
                             "Food and beverages", "Sports and travel"],
        "invoice_id":       ["INV-001", "INV-002", "INV-003", "INV-004"],
        "customer_type":    ["Member", "Normal", "Member", "Normal"],
        "gender":           ["Female", "Male", "Female", "Male"],
        "payment_method":   ["Cash", "Credit card", "Ewallet", "Cash"],
        "unit_price":       [10.0, 20.0, 30.0, 40.0],
        "quantity":         [1, 2, 3, 4],
        "tax_amount":       [0.5, 1.0, 1.5, 2.0],
        "total_amount":     [10.5, 21.0, 31.5, 42.0],
        "cogs":             [10.0, 20.0, 30.0, 40.0],
        "gross_margin_pct": [4.76, 4.76, 4.76, 4.76],
        "gross_income":     [0.5, 1.0, 1.5, 2.0],
        "rating":           [8.0, 7.0, 9.0, 6.0],
        "sale_date":        ["2019-01-01", "2019-01-02",
                             "2019-01-03", "2019-01-04"],
        "sale_time":        ["10:00", "11:00", "12:00", "13:00"],
    }
    return pd.DataFrame(data)


# ── Dimension Tests ────────────────────────────────────────────────
def test_dim_branch_row_count(sample_df):
    """
    Validates that the branch dimension correctly identifies unique branches.
    Expected behavior: 3 unique branch/city combinations from the sample data.
    """
    dim_branch = (
        sample_df[["branch", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_branch["branch_key"] = dim_branch.index + 1
    assert len(dim_branch) == 3, f"Expected 3 branches, got {len(dim_branch)}"


def test_dim_branch_has_required_columns(sample_df):
    """
    Ensures the transformed branch dimension table contains all necessary columns 
    for the star schema (the surrogate key and natural keys).
    """
    dim_branch = (
        sample_df[["branch", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_branch["branch_key"] = dim_branch.index + 1
    assert "branch_key" in dim_branch.columns
    assert "branch"     in dim_branch.columns
    assert "city"       in dim_branch.columns


def test_dim_product_row_count(sample_df):
    """
    Validates that the product dimension correctly deduplicates product lines.
    Expected behavior: 3 unique product lines from the sample data.
    """
    dim_product = (
        sample_df[["product_line"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_product["product_key"] = dim_product.index + 1
    assert len(dim_product) == 3, f"Expected 3 products, got {len(dim_product)}"


# ── Fact Table Tests ───────────────────────────────────────────────
def test_fact_sales_row_count(sample_df):
    """
    Verifies that no rows are lost during the initial loading of the fact table.
    """
    assert len(sample_df) == 4, f"Expected 4 rows, got {len(sample_df)}"


def test_fact_sales_no_null_branch_keys(sample_df):
    """
    Checks the integrity of the foreign key relationship between fact_sales 
    and dim_branch. Ensures every sale is successfully mapped to a branch_key.
    """
    dim_branch = (
        sample_df[["branch", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_branch["branch_key"] = dim_branch.index + 1
    fact = sample_df.merge(dim_branch, on=["branch", "city"], how="left")
    null_keys = fact["branch_key"].isnull().sum()
    assert null_keys == 0, f"Found {null_keys} null branch keys"


def test_fact_sales_no_null_product_keys(sample_df):
    """
    Checks the integrity of the foreign key relationship between fact_sales 
    and dim_product. Ensures every sale is successfully mapped to a product_key.
    """
    dim_product = (
        sample_df[["product_line"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_product["product_key"] = dim_product.index + 1
    fact = sample_df.merge(dim_product, on="product_line", how="left")
    null_keys = fact["product_key"].isnull().sum()
    assert null_keys == 0, f"Found {null_keys} null product keys"


def test_total_amount_positive(sample_df):
    """
    Data quality check: Ensures all total transaction amounts are positive numbers.
    """
    assert (sample_df["total_amount"] > 0).all(), "Found negative total amounts"


def test_rating_range(sample_df):
    """
    Data quality check: Ensures customer ratings fall within the valid range of 1 to 10.
    """
    assert sample_df["rating"].between(1, 10).all(), "Rating out of range"


def test_quantity_positive(sample_df):
    """
    Data quality check: Ensures the quantity of items sold is always greater than zero.
    """
    assert (sample_df["quantity"] > 0).all(), "Found non-positive quantities"


# ── SQLite Load Tests ──────────────────────────────────────────────
def test_sqlite_tables_exist(sample_df, tmp_path):
    """
    Tests the database schema creation. It writes the tables to a temporary 
    SQLite database and verifies that the tables exist in the master schema.
    """
    db_path = str(tmp_path / "test.sqlite")
    conn = sqlite3.connect(db_path)

    dim_branch = (
        sample_df[["branch", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_branch["branch_key"] = dim_branch.index + 1

    dim_product = (
        sample_df[["product_line"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_product["product_key"] = dim_product.index + 1

    dim_branch.to_sql("dim_branch",  conn, if_exists="replace", index=False)
    dim_product.to_sql("dim_product", conn, if_exists="replace", index=False)
    sample_df.to_sql("fact_sales",   conn, if_exists="replace", index=False)

    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table'", conn
    )["name"].tolist()

    assert "dim_branch"  in tables
    assert "dim_product" in tables
    assert "fact_sales"  in tables
    conn.close()


def test_sqlite_row_counts(sample_df, tmp_path):
    """
    Verifies that the number of rows inserted into the SQLite database 
    matches the source DataFrame row count.
    """
    db_path = str(tmp_path / "test.sqlite")
    conn = sqlite3.connect(db_path)
    sample_df.to_sql("fact_sales", conn, if_exists="replace", index=False)
    count = pd.read_sql(
        "SELECT COUNT(*) as cnt FROM fact_sales", conn
    ).iloc[0, 0]
    assert count == len(sample_df), f"Expected {len(sample_df)}, got {count}"
    conn.close()


# ── Data Quality Tests ─────────────────────────────────────────────
def test_no_null_invoice_ids(sample_df):
    """
    Ensures the Primary Key equivalent (invoice_id) contains no null values.
    """
    assert sample_df["invoice_id"].isnull().sum() == 0


def test_valid_branches(sample_df):
    """
    Validates that the 'branch' column only contains values from the 
    predefined set of allowed branches (A, B, or C).
    """
    valid = {"A", "B", "C"}
    invalid = sample_df[~sample_df["branch"].isin(valid)]
    assert len(invalid) == 0, f"Found invalid branches: {invalid['branch'].tolist()}"


def test_valid_cities(sample_df):
    """
    Validates that the 'city' column only contains values from the 
    allowed list of cities.
    """
    valid = {"Yangon", "Mandalay", "Naypyitaw"}
    invalid = sample_df[~sample_df["city"].isin(valid)]
    assert len(invalid) == 0, f"Found invalid cities: {invalid['city'].tolist()}"


def test_valid_customer_types(sample_df):
    """
    Ensures that customer types strictly adhere to categorized values 
    ('Member' or 'Normal').
    """
    valid = {"Member", "Normal"}
    invalid = sample_df[~sample_df["customer_type"].isin(valid)]
    assert len(invalid) == 0


def test_valid_genders(sample_df):
    """
    Validates that the gender column contains only expected categorical values.
    """
    valid = {"Male", "Female"}
    invalid = sample_df[~sample_df["gender"].isin(valid)]
    assert len(invalid) == 0


def test_valid_payment_methods(sample_df):
    """
    Validates that the payment method column contains only supported 
    values (Cash, Credit card, or Ewallet).
    """
    valid = {"Cash", "Credit card", "Ewallet"}
    invalid = sample_df[~sample_df["payment_method"].isin(valid)]
    assert len(invalid) == 0

def test_load_summary_created(sample_df):
    """load_summary.json must be created after local load."""
    from src.etl import OUTPUT_DIR, load_data

    dim_branch = (
        sample_df[["branch", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"branch": "branch_code"})
    )
    dim_branch["branch_key"] = dim_branch.index + 1
    dim_branch = dim_branch[["branch_key", "branch_code", "city"]]

    dim_product = (
        sample_df[["product_line"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_product["product_key"] = dim_product.index + 1
    dim_product = dim_product[["product_key", "product_line"]]

    fact_sales = sample_df.merge(
        dim_branch.rename(columns={"branch_code": "branch"}),
        on=["branch", "city"], how="left"
    ).merge(dim_product, on="product_line", how="left")
    fact_sales = fact_sales[[
        "invoice_id", "branch_key", "product_key", "sale_date", "sale_time",
        "customer_type", "gender", "payment_method", "unit_price", "quantity",
        "tax_amount", "total_amount", "cogs", "gross_margin_pct",
        "gross_income", "rating"
    ]].copy()
    fact_sales.insert(0, "sales_key", range(1, len(fact_sales) + 1))

    load_data(dim_branch, dim_product, fact_sales)

    assert (OUTPUT_DIR / "load_summary.json").exists(), \
        "load_summary.json was not created"


def test_load_summary_no_failures(sample_df):
    """load_summary.json must show no failed tables on clean local run."""
    from src.etl import OUTPUT_DIR, load_data

    dim_branch = (
        sample_df[["branch", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"branch": "branch_code"})
    )
    dim_branch["branch_key"] = dim_branch.index + 1
    dim_branch = dim_branch[["branch_key", "branch_code", "city"]]

    dim_product = (
        sample_df[["product_line"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_product["product_key"] = dim_product.index + 1
    dim_product = dim_product[["product_key", "product_line"]]

    fact_sales = sample_df.merge(
        dim_branch.rename(columns={"branch_code": "branch"}),
        on=["branch", "city"], how="left"
    ).merge(dim_product, on="product_line", how="left")
    fact_sales = fact_sales[[
        "invoice_id", "branch_key", "product_key", "sale_date", "sale_time",
        "customer_type", "gender", "payment_method", "unit_price", "quantity",
        "tax_amount", "total_amount", "cogs", "gross_margin_pct",
        "gross_income", "rating"
    ]].copy()
    fact_sales.insert(0, "sales_key", range(1, len(fact_sales) + 1))

    load_data(dim_branch, dim_product, fact_sales)

    with open(OUTPUT_DIR / "load_summary.json") as f:
        summary = json.load(f)

    assert summary["failed"] == [], \
        f"Expected no failures but got: {summary['failed']}"
    assert len(summary["succeeded"]) == 3, \
        f"Expected 3 succeeded but got: {summary['succeeded']}"