-- check null customer_id
SELECT *
FROM bronze_customers
WHERE customer_id IS NULL;
