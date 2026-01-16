-- Duplicate customers
SELECT customer_id, COUNT(*)
FROM silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
