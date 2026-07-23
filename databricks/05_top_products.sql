%sql
-- ==========================================================
-- Business Question 5
-- Which products consistently rank in the Top 10 by monthly
-- revenue?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_top_10_monthly_products AS

-- CTE: Calculate monthly revenue for each product
WITH product_monthly_sales AS (

    SELECT

        d.Year,
        d.Month,
        p.ProductKey,
        p.ProductName,

        -- Aggregation: Calculate monthly revenue
        SUM(f.LineTotal) AS MonthlyRevenue

    FROM gold.fact_sales f

    -- Join product dimension to get product name
    JOIN gold.dim_product p
        ON f.ProductKey = p.ProductKey

    -- Join date dimension to get year and month
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate revenue by month and product
    GROUP BY
        d.Year,
        d.Month,
        p.ProductKey,
        p.ProductName

),

-- Window Function + DENSE_RANK():
-- Rank products by monthly revenue
ranked_products AS (

    SELECT

        Year,
        Month,
        ProductKey,
        ProductName,
        MonthlyRevenue,

        DENSE_RANK() OVER (
            PARTITION BY Year, Month
            ORDER BY MonthlyRevenue DESC
        ) AS RevenueRank

    FROM product_monthly_sales

)

SELECT

    Year,
    Month,
    ProductKey,
    ProductName,
    MonthlyRevenue,
    RevenueRank

FROM ranked_products

-- Keep only the Top 10 products each month
WHERE RevenueRank <= 10;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_top_10_monthly_products

-- Sort results by month and rank
ORDER BY
    Year,
    Month,
    RevenueRank,
    MonthlyRevenue DESC;
