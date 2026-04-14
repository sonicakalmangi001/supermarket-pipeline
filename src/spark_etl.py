import os
import json

os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
os.environ["PATH"] = os.environ.get("JAVA_HOME", "") + "/bin:" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import kagglehub

OUTPUT_PATH = "./spark_output"
DATASET_ID = "lovishbansal123/sales-of-a-supermarket"
TOLERANCE = 0.02

spark = (
    SparkSession.builder
    .appName("SupermarketSalesETLWithDQ")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def clean_col_name(name: str) -> str:
    return (
        name.strip().lower()
        .replace(" ", "_")
        .replace("%", "pct")
        .replace("-", "_")
    )


def main():
    dataset_dir = kagglehub.dataset_download(DATASET_ID)
    csv_files = []
    for root, _, files in os.walk(dataset_dir):
        for f in files:
            if f.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, f))
    if not csv_files:
        raise FileNotFoundError("No CSV found in dataset download")
    csv_path = csv_files[0]

    df_raw = spark.read.csv(csv_path, header=True, inferSchema=True)

    required_columns = [
        "Invoice ID", "Branch", "City", "Customer type", "Gender", "Product line",
        "Unit price", "Quantity", "Tax 5%", "Total", "Date", "Time", "Payment",
        "cogs", "gross margin percentage", "gross income", "Rating"
    ]
    missing_columns = [c for c in required_columns if c not in df_raw.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    metrics = {}
    metrics["raw_row_count"] = df_raw.count()
    metrics["raw_duplicate_rows"] = df_raw.count() - df_raw.dropDuplicates().count()
    metrics["duplicate_invoice_ids"] = df_raw.groupBy("Invoice ID").count().filter(F.col("count") > 1).count()

    null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required_columns]
    null_counts_row = df_raw.select(*null_exprs).collect()[0].asDict()
    metrics["null_counts"] = {k: int(v) for k, v in null_counts_row.items()}

    df = df_raw
    for old_col in df.columns:
        df = df.withColumnRenamed(old_col, clean_col_name(old_col))

    rename_map = {
        "tax_5pct": "tax_amount",
        "total": "total_amount",
        "date": "sale_date",
        "time": "sale_time",
        "payment": "payment_method",
        "invoice_id": "invoice_id",
        "customer_type": "customer_type",
        "product_line": "product_line",
        "unit_price": "unit_price",
        "gross_margin_percentage": "gross_margin_pct",
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    df = (
        df.withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))
          .withColumn("sale_time", F.to_timestamp(F.col("sale_time"), "HH:mm"))
    )

    df = df.withColumn("dq_invalid_date", F.col("sale_date").isNull())
    df = df.withColumn("dq_invalid_time", F.col("sale_time").isNull())
    df = df.withColumn("dq_invalid_branch", ~F.col("branch").isin("A", "B", "C"))
    df = df.withColumn("dq_invalid_city", ~F.col("city").isin("Yangon", "Mandalay", "Naypyitaw"))
    df = df.withColumn("dq_invalid_customer_type", ~F.col("customer_type").isin("Member", "Normal"))
    df = df.withColumn("dq_invalid_gender", ~F.col("gender").isin("Male", "Female"))
    df = df.withColumn("dq_invalid_payment", ~F.col("payment_method").isin("Cash", "Credit card", "Ewallet"))
    df = df.withColumn(
        "dq_invalid_numeric",
        (F.col("unit_price") <= 0) |
        (F.col("quantity") < 1) |
        (F.col("rating") < 0) |
        (F.col("rating") > 10) |
        F.col("unit_price").isNull() |
        F.col("quantity").isNull() |
        F.col("rating").isNull()
    )
    df = df.withColumn("dq_tax_expected", F.round(F.col("cogs") * F.lit(0.05), 2))
    df = df.withColumn("dq_total_expected", F.round(F.col("cogs") + F.col("tax_amount"), 2))
    df = df.withColumn("dq_cogs_expected", F.round(F.col("unit_price") * F.col("quantity"), 2))
    df = df.withColumn("dq_tax_mismatch", F.abs(F.col("tax_amount") - F.col("dq_tax_expected")) > TOLERANCE)
    df = df.withColumn("dq_total_mismatch", F.abs(F.col("total_amount") - F.col("dq_total_expected")) > TOLERANCE)
    df = df.withColumn("dq_cogs_mismatch", F.abs(F.col("cogs") - F.col("dq_cogs_expected")) > TOLERANCE)
    df = df.withColumn("dq_gross_income_mismatch", F.abs(F.col("gross_income") - F.col("tax_amount")) > TOLERANCE)

    critical_cols = ["invoice_id", "branch", "city", "product_line", "unit_price", "quantity", "total_amount", "sale_date"]
    critical_null_expr = None
    for c in critical_cols:
        expr = F.col(c).isNull()
        critical_null_expr = expr if critical_null_expr is None else (critical_null_expr | expr)
    df = df.withColumn("dq_critical_null", critical_null_expr)

    reject_expr = (
        F.col("dq_critical_null") |
        F.col("dq_invalid_date") |
        F.col("dq_invalid_time") |
        F.col("dq_invalid_branch") |
        F.col("dq_invalid_city") |
        F.col("dq_invalid_customer_type") |
        F.col("dq_invalid_gender") |
        F.col("dq_invalid_payment") |
        F.col("dq_invalid_numeric")
    )
    df = df.withColumn("dq_rejected", reject_expr)

    agg_row = df.agg(
        F.sum(F.col("dq_invalid_date").cast("int")).alias("invalid_dates"),
        F.sum(F.col("dq_invalid_time").cast("int")).alias("invalid_times"),
        F.sum(F.col("dq_invalid_branch").cast("int")).alias("invalid_branch_values"),
        F.sum(F.col("dq_invalid_city").cast("int")).alias("invalid_city_values"),
        F.sum(F.col("dq_invalid_customer_type").cast("int")).alias("invalid_customer_type_values"),
        F.sum(F.col("dq_invalid_gender").cast("int")).alias("invalid_gender_values"),
        F.sum(F.col("dq_invalid_payment").cast("int")).alias("invalid_payment_values"),
        F.sum(F.col("dq_invalid_numeric").cast("int")).alias("invalid_numeric_rows"),
        F.sum(F.col("dq_tax_mismatch").cast("int")).alias("tax_mismatch_rows"),
        F.sum(F.col("dq_total_mismatch").cast("int")).alias("total_mismatch_rows"),
        F.sum(F.col("dq_cogs_mismatch").cast("int")).alias("cogs_mismatch_rows"),
        F.sum(F.col("dq_gross_income_mismatch").cast("int")).alias("gross_income_mismatch_rows"),
        F.sum(F.col("dq_critical_null").cast("int")).alias("critical_null_rows"),
        F.sum(F.col("dq_rejected").cast("int")).alias("rejected_rows")
    ).collect()[0].asDict()
    metrics.update({k: int(v) for k, v in agg_row.items()})

    rejects = df.filter(F.col("dq_rejected") | F.col("dq_tax_mismatch") | F.col("dq_total_mismatch") | F.col("dq_cogs_mismatch") | F.col("dq_gross_income_mismatch"))
    rejects.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUTPUT_PATH}/data_quality_rejects")

    clean_df = df.filter(~F.col("dq_rejected")).dropDuplicates(["invoice_id"])
    metrics["clean_rows_after_invoice_dedup"] = clean_df.count()

    dim_branch = clean_df.select("branch", "city").distinct().orderBy("branch", "city")
    window_branch = Window.orderBy("branch", "city")
    dim_branch = dim_branch.withColumn("branch_key", F.row_number().over(window_branch)).select("branch_key", "branch", "city")

    dim_product = clean_df.select("product_line").distinct().orderBy("product_line")
    window_product = Window.orderBy("product_line")
    dim_product = dim_product.withColumn("product_key", F.row_number().over(window_product)).select("product_key", "product_line")

    fact_sales = (
        clean_df.join(dim_branch, on=["branch", "city"], how="left")
                .join(dim_product, on="product_line", how="left")
                .select(
                    "invoice_id", "branch_key", "product_key", "sale_date", "sale_time",
                    "customer_type", "gender", "payment_method", "unit_price", "quantity",
                    "tax_amount", "total_amount", "cogs", "gross_margin_pct", "gross_income", "rating"
                )
    )
    window_fact = Window.orderBy("invoice_id")
    fact_sales = fact_sales.withColumn("sales_key", F.row_number().over(window_fact)).select(
        "sales_key", "invoice_id", "branch_key", "product_key", "sale_date", "sale_time",
        "customer_type", "gender", "payment_method", "unit_price", "quantity",
        "tax_amount", "total_amount", "cogs", "gross_margin_pct", "gross_income", "rating"
    )

    dim_branch.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/dim_branch")
    dim_product.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/dim_product")
    fact_sales.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/fact_sales")

    with open(f"{OUTPUT_PATH}/data_quality_report.json", "w") as f:
        json.dump(metrics, f, indent=2)

    fact_sales.createOrReplaceTempView("fact_sales")
    dim_branch.createOrReplaceTempView("dim_branch")
    dim_product.createOrReplaceTempView("dim_product")

    report_df = spark.sql("""
        SELECT
            b.branch AS branch_code,
            b.city,
            p.product_line,
            COUNT(f.sales_key) AS transaction_count,
            SUM(f.quantity) AS total_units_sold,
            ROUND(SUM(f.total_amount), 2) AS total_sales,
            ROUND(SUM(f.gross_income), 2) AS total_gross_income,
            ROUND(AVG(f.rating), 2) AS avg_rating,
            RANK() OVER (
                PARTITION BY b.branch
                ORDER BY SUM(f.total_amount) DESC
            ) AS sales_rank_within_branch,
            ROUND(
                100.0 * SUM(f.total_amount) /
                SUM(SUM(f.total_amount)) OVER (PARTITION BY b.branch),
                2
            ) AS pct_of_branch_sales
        FROM fact_sales f
        JOIN dim_branch b ON f.branch_key = b.branch_key
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY b.branch, b.city, p.product_line
        ORDER BY b.branch, sales_rank_within_branch, p.product_line
    """)
    report_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUTPUT_PATH}/report")

    print("Spark ETL complete with data quality checks")
    print(json.dumps(metrics, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
