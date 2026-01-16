-- No null foreign keys
SELECT *
FROM gold.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL;


-- Sales amount validation
SELECT *
FROM gold.fact_sales
WHERE sales_amount <> quantity * price;
