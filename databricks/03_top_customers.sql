%sql
-- ==========================================================
-- Business Question 3
-- Who are the top 20 customers by lifetime value, and what
-- percentage of total company revenue do they represent?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_top_20_customers_lifetime_value AS

-- CTE: Calculate lifetime revenue for each customer
WITH customer_revenue AS (

    SELECT

        c.CustomerKey,
        c.CustomerName,

        -- Aggregation: Calculate lifetime revenue
        SUM(f.LineTotal) AS LifetimeRevenue

    FROM gold.fact_sales f

    -- Join customer dimension to get customer name
    JOIN gold.dim_customer c
        ON f.CustomerKey = c.CustomerKey

    -- GROUP BY: Aggregate revenue by customer
    GROUP BY
        c.CustomerKey,
        c.CustomerName

)

SELECT

    CustomerKey,
    CustomerName,
    LifetimeRevenue,

    -- Window Function + Percentage Calculation:
    -- Calculate each customer's percentage of total company revenue
    ROUND(
        LifetimeRevenue * 100.0 /
        SUM(LifetimeRevenue) OVER (),
        2
    ) AS RevenuePercentage,

    -- Window Function:
    -- Calculate the cumulative revenue percentage
    ROUND(
        SUM(LifetimeRevenue) OVER (
            ORDER BY LifetimeRevenue DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0
        /
        SUM(LifetimeRevenue) OVER (),
        2
    ) AS CumulativeRevenuePercentage

FROM customer_revenue;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_top_20_customers_lifetime_value

-- Show the top 20 customers by lifetime revenue
ORDER BY
    LifetimeRevenue DESC

LIMIT 20;
