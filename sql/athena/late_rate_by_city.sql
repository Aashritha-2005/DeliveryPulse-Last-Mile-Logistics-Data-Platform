-- Late-delivery rate by city over the cleaned orders in the lake (Amazon Athena, Trino SQL).
-- Same rule as the PostgreSQL analytics: late = delivered with delivery_ts > promised_ts
-- (the cleaned layer's is_late); rate = late delivered / delivered. Cancelled orders excluded.
-- The table is resolved in the workgroup's Glue database (QueryExecutionContext).
SELECT city,
       count(*) AS delivered_orders,
       count_if(is_late) AS late_orders,
       round(CAST(count_if(is_late) AS double) / count(*), 4) AS late_rate
FROM cleaned_orders
WHERE status = 'delivered'
GROUP BY city
ORDER BY city
