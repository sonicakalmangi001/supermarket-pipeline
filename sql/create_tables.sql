CREATE TABLE dim_branch (
    branch_key INTEGER PRIMARY KEY,
    branch_code TEXT UNIQUE,
    city TEXT
);

CREATE TABLE dim_product (
    product_key INTEGER PRIMARY KEY,
    product_line TEXT UNIQUE
);

CREATE TABLE fact_sales (
    sales_key INTEGER PRIMARY KEY,
    invoice_id TEXT UNIQUE,
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
);

-- Staging tables
CREATE TABLE IF NOT EXISTS dim_branch_staging (
  branch_key INTEGER,
  branch_code TEXT,
  city TEXT
);

CREATE TABLE IF NOT EXISTS dim_product_staging (
  product_key INTEGER,
  product_line TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales_staging (
  sales_key INTEGER,
  invoice_id TEXT UNIQUE,
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
  rating REAL
);