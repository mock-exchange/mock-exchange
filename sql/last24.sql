-- Open, High, Low, Close over last 24h
SELECT
    open,
    MAX(price) AS high,
    MIN(price) AS low,
    close,
    SUM(amount) AS volume,
    AVG(price) AS avg_price,
    ((close - open) / open) AS change
FROM (
SELECT
    price,
    amount,
    CAST(first_value(price) OVER win AS REAL) AS open,
    CAST(last_value(price) OVER win AS REAL) AS close
FROM trade
WHERE
    market_id = ?
    AND created BETWEEN datetime(created, '-1 day') AND datetime()
WINDOW win as (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
