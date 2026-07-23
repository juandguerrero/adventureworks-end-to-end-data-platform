%sql
-- ==========================================================
-- Business Question 10
-- What is the average time between customer purchases,
-- and how does it vary by customer segment?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_avg_purchase_interval_by_segment AS

-- CTE: Get each customer's purchase history
WITH customer_orders AS (

    SELECT DISTINCT

        f.CustomerKey,
        TO_DATE(CAST(d.DateKey AS STRING), 'yyyyMMdd') AS OrderDate,
        f.LineTotal

    FROM gold.fact_sales f

    -- Join date dimension
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

),

-- CTE: Calculate days between purchases
purchase_intervals AS (

    SELECT

        CustomerKey,
        OrderDate,

        -- Window Function + LAG():
        -- Get the previous purchase date
        LAG(OrderDate) OVER (
            PARTITION BY CustomerKey
            ORDER BY OrderDate
        ) AS PreviousPurchaseDate,

        -- Date Function:
        -- Calculate days between purchases
        DATEDIFF(
            OrderDate,
            LAG(OrderDate) OVER (
                PARTITION BY CustomerKey
                ORDER BY OrderDate
            )
        ) AS DaysBetweenPurchases,

        LineTotal

    FROM customer_orders

),

-- CTE: Calculate customer lifetime revenue
customer_segment AS (

    SELECT

        CustomerKey,

        -- Aggregation: Calculate lifetime revenue
        SUM(LineTotal) AS LifetimeRevenue,

        -- CASE Expression:
        -- Classify customers into segments
        CASE

            WHEN SUM(LineTotal) >= 10000 THEN 'High Value'
            WHEN SUM(LineTotal) >= 5000 THEN 'Medium Value'
            ELSE 'Low Value'

        END AS CustomerSegment

    FROM customer_orders

    -- GROUP BY: Aggregate revenue by customer
    GROUP BY
        CustomerKey

)

SELECT

    s.CustomerSegment,

    -- Aggregation: Calculate the average days between purchases
    ROUND(AVG(p.DaysBetweenPurchases), 2) AS AvgDaysBetweenPurchases

FROM purchase_intervals p

-- Join customer segments
JOIN customer_segment s
    ON p.CustomerKey = s.CustomerKey

-- Ignore the first purchase for each customer
WHERE p.DaysBetweenPurchases IS NOT NULL

-- GROUP BY: Calculate averages by customer segment
GROUP BY
    s.CustomerSegment;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_avg_purchase_interval_by_segment

-- Show segments with the shortest purchase intervals first
ORDER BY
    AvgDaysBetweenPurchases;
