-- Open, High, Low, Close by time
SELECT
    DISTINCT date(created) AS time,
    first_value(price) over w AS open,
    MAX(price) over w AS high,
    MIN(price) over w AS low,
    last_value(price) over w AS close,
    CAST(SUM(amount) over w AS INT) AS value
FROM trade
WHERE market = ?
window w AS (partition BY date(created))

