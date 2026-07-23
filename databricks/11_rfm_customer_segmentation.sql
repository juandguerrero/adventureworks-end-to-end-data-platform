%sql
-- ==========================================================
-- Business Question 11
-- Which products generate high revenue but also have
-- low profitability?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_high_revenue_low_profit_products AS

-- CTE: Calculate sales and profit metrics for each product
WITH product_profitability AS (

    SELECT

        p.ProductKey,
        p.ProductName,

        -- Aggregation: Calculate total revenue
        SUM(f.LineTotal) AS TotalRevenue,

        -- Conditional Aggregation: Calculate total cost
        SUM(f.OrderQty * p.StandardCost) AS TotalCost,

        -- Calculated Metric: Calculate total profit
        SUM(f.LineTotal) -
        SUM(f.OrderQty * p.StandardCost) AS TotalProfit

    FROM gold.fact_sales f

    -- Join product dimension
    JOIN gold.dim_product p
        ON f.ProductKey = p.ProductKey

    -- GROUP BY: Aggregate metrics by product
    GROUP BY
        p.ProductKey,
        p.ProductName

)

SELECT

    ProductName,
    TotalRevenue,
    TotalCost,
    TotalProfit,

    -- Calculated Metric:
    -- Calculate profit margin percentage
    ROUND(
        TotalProfit * 100.0 / TotalRevenue,
        2
    ) AS ProfitMargin

FROM product_profitability

-- Keep products with high revenue but low profitability
WHERE
    TotalRevenue > 100000
    AND
    ROUND(
        TotalProfit * 100.0 / TotalRevenue,
        2
    ) < 20;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_high_revenue_low_profit_products

-- Show highest revenue products first
ORDER BY
    TotalRevenue DESC;
