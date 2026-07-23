%sql
-- ==========================================================
-- Business Question 12
-- Which months show unusual sales spikes or drops compared
-- to historical averages?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_monthly_sales_trends AS

-- CTE: Calculate total sales for each month
WITH monthly_sales AS (

    SELECT

        d.Year,
        d.Month,
        CONCAT(d.Year, '-', LPAD(d.Month, 2, '0')) AS YearMonth,

        -- Aggregation: Calculate monthly sales
        SUM(f.LineTotal) AS MonthlySales

    FROM gold.fact_sales f

    -- Join date dimension
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- GROUP BY: Aggregate sales by month
    GROUP BY
        d.Year,
        d.Month

)

SELECT

    Year,
    Month,
    YearMonth,
    MonthlySales,

    -- Window Function:
    -- Calculate the 3-month moving average
    ROUND(
        AVG(MonthlySales) OVER (
            ORDER BY Year, Month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS MovingAverage,

    -- Statistical Comparison:
    -- Compare monthly sales to the moving average
    CASE

        WHEN MonthlySales >
            AVG(MonthlySales) OVER (
                ORDER BY Year, Month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )
        THEN 'Sales Spike'

        WHEN MonthlySales <
            AVG(MonthlySales) OVER (
                ORDER BY Year, Month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )
        THEN 'Sales Drop'

        ELSE 'Normal'

    END AS SalesTrend

FROM monthly_sales;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_monthly_sales_trends

-- Show results in chronological order
ORDER BY
    Year,
    Month;
