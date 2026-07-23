%sql
-- ==========================================================
-- Business Question 1
-- What are the monthly sales and month-over-month growth rate
-- over the last three years?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_monthly_sales_growth AS

-- CTE: Calculate total sales for each month
WITH monthly_sales AS (

    SELECT

        d.Year,
        d.Month,
        CONCAT(d.Year, '-', LPAD(d.Month, 2, '0')) AS YearMonth,

        -- Aggregation: Calculate monthly sales
        SUM(f.LineTotal) AS MonthlySales

    FROM gold.fact_sales f

    -- Join date dimension to get year and month
    JOIN gold.dim_date d
        ON f.DateKey = d.DateKey

    -- Date Function: Keep only the last three years
    WHERE d.Year >= (
        SELECT MAX(Year) - 2
        FROM gold.dim_date
    )

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

    -- Window Function + LAG():
    -- Get the previous month's sales
    LAG(MonthlySales) OVER (
        ORDER BY Year, Month
    ) AS PreviousMonthSales,

    -- Calculate month-over-month growth percentage
    ROUND(
        (
            MonthlySales -
            LAG(MonthlySales) OVER (
                ORDER BY Year, Month
            )
        ) * 100.0
        /
        LAG(MonthlySales) OVER (
            ORDER BY Year, Month
        ),
        2
    ) AS MoMGrowthRate

FROM monthly_sales;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_monthly_sales_growth

-- Show results in chronological order
ORDER BY
    Year,
    Month;


