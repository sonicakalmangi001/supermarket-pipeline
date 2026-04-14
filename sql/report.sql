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
        100.0 * SUM(f.total_amount)
        / SUM(SUM(f.total_amount)) OVER (PARTITION BY b.branch_code),
        2
    ) AS pct_of_branch_sales
FROM fact_sales f
JOIN dim_branch b ON f.branch_key = b.branch_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY b.branch_code, b.city, p.product_line
ORDER BY b.branch_code, sales_rank_within_branch, p.product_line;
