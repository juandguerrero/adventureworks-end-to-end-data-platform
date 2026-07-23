%sql
-- ==========================================================
-- Business Question 6
-- Which products are frequently purchased together
-- in the same order?
-- ==========================================================

-- ==========================================================
-- Create View
-- ==========================================================
CREATE OR REPLACE VIEW gold.vw_products_frequently_purchased_together AS

-- CTE: Generate product pairs from the same order
WITH product_pairs AS (

    SELECT

        f1.ProductKey AS Product1Key,
        p1.ProductName AS Product1,

        f2.ProductKey AS Product2Key,
        p2.ProductName AS Product2

    FROM gold.fact_sales f1

    -- Self Join: Match products from the same order
    JOIN gold.fact_sales f2
        ON f1.SalesOrderID = f2.SalesOrderID

        -- Pair Generation: Avoid duplicate and reversed pairs
        AND f1.ProductKey < f2.ProductKey

    -- Join product dimension to get first product name
    JOIN gold.dim_product p1
        ON f1.ProductKey = p1.ProductKey

    -- Join product dimension to get second product name
    JOIN gold.dim_product p2
        ON f2.ProductKey = p2.ProductKey

)

SELECT

    Product1,
    Product2,

    -- Aggregation: Count how many orders contain the pair
    COUNT(*) AS TimesPurchasedTogether

FROM product_pairs

-- GROUP BY: Aggregate by product pair
GROUP BY
    Product1,
    Product2;

-- ==========================================================
-- Query the View
-- ==========================================================

SELECT *

FROM gold.vw_products_frequently_purchased_together

-- Show the most frequently purchased pairs first
ORDER BY
    TimesPurchasedTogether DESC;
