%sql
-- ==========================================================
-- Business Question 2
-- Which product categories contribute the highest percentage
-- of total revenue each year?
-- ==========================================================

-- ==========================================================
-- Create View  
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_category_revenue_percentage AS

-- CTE: Calculate total revenue by category and year
WITH category_sales AS (

    SELECT

        d.Year,
        p.Category,

        -- Aggregation: Calculate total revenue
        SUM(f.LineTotal) AS TotalRevenue

    FROM gold.fact_sales f

    -- Join product dimension to get category
    JOIN gold.dim_product p
        ON f.ProductKey = p.ProductKey

    -- Join date dimension to get year
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate revenue by year and category
    GROUP BY
        d.Year,
        p.Category

)

SELECT

    Year,
    Category,
    TotalRevenue,

    -- Window Function + Percentage Calculation:
    -- Calculate each category's share of yearly revenue
    ROUND(
        TotalRevenue * 100.0 /
        SUM(TotalRevenue) OVER (PARTITION BY Year),
        2
    ) AS RevenuePercentage

FROM category_sales;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_category_revenue_percentage

-- Sort categories by highest revenue percentage each year
ORDER BY
    Year,
    RevenuePercentage DESC;
