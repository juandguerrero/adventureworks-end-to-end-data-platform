%sql
-- ==========================================================
-- Business Question 14
-- Which customers moved from low-value to high-value
-- segments over time?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_customer_segment_progression AS

-- CTE: Calculate yearly revenue for each customer
WITH customer_sales AS (

    SELECT

        d.Year,
        c.CustomerKey,
        c.CustomerName,

        -- Aggregation: Calculate yearly revenue
        SUM(f.LineTotal) AS TotalRevenue

    FROM gold.fact_sales f

    -- Join customer dimension
    JOIN gold.dim_customer c
        ON f.CustomerKey = c.CustomerKey

    -- Join date dimension
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate revenue by customer and year
    GROUP BY
        d.Year,
        c.CustomerKey,
        c.CustomerName

),

-- CTE: Assign customers to value segments
customer_segments AS (

    SELECT

        Year,
        CustomerKey,
        CustomerName,
        TotalRevenue,

        -- Window Function + NTILE():
        -- Divide customers into four revenue segments each year
        NTILE(4) OVER (
            PARTITION BY Year
            ORDER BY TotalRevenue
        ) AS CustomerSegment

    FROM customer_sales

),

-- CTE: Compare customer segments over time
segment_changes AS (

    SELECT

        Year,
        CustomerKey,
        CustomerName,
        TotalRevenue,
        CustomerSegment,

        -- Window Function + LAG():
        -- Get the customer's previous year's segment
        LAG(CustomerSegment) OVER (
            PARTITION BY CustomerKey
            ORDER BY Year
        ) AS PreviousSegment

    FROM customer_segments

)

SELECT

    Year,
    CustomerName,
    PreviousSegment,
    CustomerSegment,
    TotalRevenue

FROM segment_changes

-- Keep customers who moved to a higher segment
WHERE PreviousSegment IS NOT NULL
AND CustomerSegment > PreviousSegment;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_customer_segment_progression

-- Show largest improvements first
ORDER BY
    Year,
    CustomerSegment DESC,
    TotalRevenue DESC;
