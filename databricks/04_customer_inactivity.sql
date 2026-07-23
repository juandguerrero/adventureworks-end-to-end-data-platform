%sql
-- ==========================================================
-- Business Question 4
-- Which customers have not purchased anything in the last
-- 12 months but were previously active?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_inactive_customers_last_12_months AS

SELECT

    c.CustomerKey,
    c.CustomerName

FROM gold.dim_customer c

-- Subquery:
-- Return only customers who have made at least one purchase
WHERE EXISTS (

    SELECT 1

    FROM gold.fact_sales f
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    WHERE f.CustomerKey = c.CustomerKey

)

-- NOT EXISTS:
-- Exclude customers who purchased during the last 12 months
AND NOT EXISTS (

    SELECT 1

    FROM gold.fact_sales f
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    WHERE f.CustomerKey = c.CustomerKey

    -- Date Function:
    -- Keep purchases made in the last 12 months
    AND TO_DATE(CAST(d.DateKey AS STRING), 'yyyyMMdd') >= DATEADD(
        MONTH,
        -12,
        (
            SELECT MAX(
                TO_DATE(CAST(DateKey AS STRING), 'yyyyMMdd')
            )
            FROM gold.dim_date
        )
    )

);

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_inactive_customers_last_12_months

-- Sort customers alphabetically
ORDER BY
    CustomerName;
