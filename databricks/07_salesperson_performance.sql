%sql
-- ==========================================================
-- Business Question 7
-- Which sales territories have experienced the highest
-- year-over-year growth?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_territory_yoy_growth AS

-- CTE: Calculate total revenue for each territory by year
WITH territory_sales AS (

    SELECT

        d.Year,
        t.TerritoryKey,
        t.Territory,

        -- Aggregation: Calculate yearly revenue
        SUM(f.LineTotal) AS TotalRevenue

    FROM gold.fact_sales f

    -- Join territory dimension to get territory name
    JOIN gold.dim_territory t
        ON f.TerritoryKey = t.TerritoryKey

    -- Join date dimension to get year
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate revenue by year and territory
    GROUP BY
        d.Year,
        t.TerritoryKey,
        t.Territory

)

SELECT

    Year,
    Territory,
    TotalRevenue,

    -- Window Function + LAG():
    -- Get the previous year's revenue for each territory
    LAG(TotalRevenue) OVER (
        PARTITION BY TerritoryKey
        ORDER BY Year
    ) AS PreviousYearRevenue,

    -- Calculate year-over-year growth percentage
    ROUND(
        (
            TotalRevenue -
            LAG(TotalRevenue) OVER (
                PARTITION BY TerritoryKey
                ORDER BY Year
            )
        ) * 100.0
        /
        LAG(TotalRevenue) OVER (
            PARTITION BY TerritoryKey
            ORDER BY Year
        ),
        2
    ) AS YoYGrowthRate

FROM territory_sales;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_territory_yoy_growth

-- Show territories with the highest growth first
ORDER BY
    YoYGrowthRate DESC;
