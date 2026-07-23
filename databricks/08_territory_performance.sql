%sql
-- ==========================================================
-- Business Question 8
-- Which salespeople consistently outperform the average
-- sales performance of their territory?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_salespeople_above_territory_average AS

-- CTE: Calculate total revenue for each salesperson
WITH salesperson_sales AS (

    SELECT

        s.SalesPersonKey,
        s.SalesPersonName,
        t.Territory,
        t.TerritoryKey,

        -- Aggregation: Calculate total sales by salesperson
        SUM(f.LineTotal) AS TotalRevenue

    FROM gold.fact_sales f

    -- Join salesperson dimension
    JOIN gold.dim_salesperson s
        ON f.SalesPersonKey = s.SalesPersonKey

    -- Join territory dimension
    JOIN gold.dim_territory t
        ON f.TerritoryKey = t.TerritoryKey

    -- GROUP BY: Aggregate sales by salesperson and territory
    GROUP BY
        s.SalesPersonKey,
        s.SalesPersonName,
        t.TerritoryKey,
        t.Territory

),

-- Window Function + AVG():
-- Calculate the average revenue for each territory
territory_average AS (

    SELECT

        *,
        AVG(TotalRevenue) OVER (
            PARTITION BY TerritoryKey
        ) AS TerritoryAverageRevenue

    FROM salesperson_sales

)

SELECT

    SalesPersonName,
    Territory,
    TotalRevenue,
    TerritoryAverageRevenue

FROM territory_average

-- Keep only salespeople above their territory average
WHERE TotalRevenue > TerritoryAverageRevenue;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_salespeople_above_territory_average

-- Show the highest-performing salespeople first
ORDER BY
    TotalRevenue DESC;
