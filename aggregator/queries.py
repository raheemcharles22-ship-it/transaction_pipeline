SUMMARY_QUERY = """
SELECT
    merchant_id,
    date_trunc('hour', occurred_at) AS window_start,
    sum(amount_cents) AS total_amount_cents,
    count(*) AS transaction_count
FROM transactions
WHERE merchant_id = $1
GROUP BY merchant_id, date_trunc('hour', occurred_at)
ORDER BY window_start
"""

ANOMALY_QUERY = """
WITH hourly_totals AS (
    SELECT
        merchant_id,
        date_trunc('hour', occurred_at) AS window_start,
        sum(amount_cents) AS total_amount_cents
    FROM transactions
    GROUP BY merchant_id, date_trunc('hour', occurred_at)
),
with_trailing_avg AS (
    SELECT
        merchant_id,
        window_start,
        total_amount_cents,
        avg(total_amount_cents) OVER (
            PARTITION BY merchant_id
            ORDER BY window_start
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) AS trailing_avg_cents
    FROM hourly_totals
)
SELECT *
FROM with_trailing_avg
WHERE trailing_avg_cents IS NOT NULL
"""