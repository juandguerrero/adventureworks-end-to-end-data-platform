%sql
-- ==========================================================
-- Business Question 9
-- Which customers are at risk of churn based on purchase
-- frequency and recency?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_customer_churn_risk AS

-- CTE: Find each customer's last purchase date
WITH customer_activity AS (

    SELECT

        f.CustomerKey,

        -- Aggregation: Count total purchases
        COUNT(DISTINCT f.SalesOrderID) AS PurchaseFrequency,

        -- Aggregation: Find the most recent purchase
        MAX(TO_DATE(CAST(d.DateKey AS STRING), 'yyyyMMdd')) AS LastPurchaseDate

    FROM gold.fact_sales f

    -- Join date dimension
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate by customer
    GROUP BY
        f.CustomerKey

),

-- CTE: Calculate customer recency
customer_recency AS (

    SELECT

        CustomerKey,
        PurchaseFrequency,
        LastPurchaseDate,

        -- Date Function: Calculate days since last purchase
        DATEDIFF(
            (
                SELECT MAX(
                    TO_DATE(CAST(DateKey AS STRING), 'yyyyMMdd')
                )
                FROM gold.dim_date
            ),
            LastPurchaseDate
        ) AS DaysSinceLastPurchase

    FROM customer_activity

)

SELECT

    c.CustomerKey,
    c.CustomerName,
    PurchaseFrequency,
    LastPurchaseDate,
    DaysSinceLastPurchase,

    -- CASE Expression:
    -- Classify customers by churn risk
    CASE

        WHEN DaysSinceLastPurchase > 365
            THEN 'High Risk'

        WHEN DaysSinceLastPurchase > 180
            THEN 'Medium Risk'

        ELSE 'Low Risk'

    END AS ChurnRisk

FROM customer_recency r

-- Join customer dimension
JOIN gold.dim_customer c
    ON r.CustomerKey = c.CustomerKey;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_customer_churn_risk

-- Show highest-risk customers first
ORDER BY
    DaysSinceLastPurchase DESC;
