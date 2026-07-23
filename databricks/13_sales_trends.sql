%sql
-- ==========================================================
-- Business Question 13
-- What are the sales trends by weekday and season, and are
-- there recurring seasonal patterns?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_sales_trends_by_weekday_season AS

-- CTE: Calculate sales by weekday and season
WITH seasonal_sales AS (

    SELECT

        d.WeekDay,

        -- CASE Expression:
        -- Assign each month to a season
        CASE

            WHEN d.Month IN (12, 1, 2)
                THEN 'Winter'

            WHEN d.Month IN (3, 4, 5)
                THEN 'Spring'

            WHEN d.Month IN (6, 7, 8)
                THEN 'Summer'

            ELSE 'Autumn'

        END AS Season,

        -- Aggregation: Calculate total sales
        SUM(f.LineTotal) AS TotalSales

    FROM gold.fact_sales f

    -- Join date dimension
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate sales by weekday and season
    GROUP BY

        d.WeekDay,

        CASE

            WHEN d.Month IN (12, 1, 2)
                THEN 'Winter'

            WHEN d.Month IN (3, 4, 5)
                THEN 'Spring'

            WHEN d.Month IN (6, 7, 8)
                THEN 'Summer'

            ELSE 'Autumn'

        END

)

SELECT

    WeekDay,
    Season,
    TotalSales

FROM seasonal_sales;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_sales_trends_by_weekday_season

-- Show highest sales first
ORDER BY
    TotalSales DESC;
