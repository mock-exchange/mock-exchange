-- Open, High, Low, Close over last 24h
SELECT
    m.id AS market_id,
    m.name,
    open,
    MAX(price) AS high,
    MIN(price) AS low,
    close,
    SUM(amount) AS volume,
    AVG(price) AS avg_price,
    ((close - open) / open) AS change
FROM market AS m
LEFT JOIN (
    SELECT
        DISTINCT market_id,
        price,
        amount,
        CAST(first_value(price) OVER win AS REAL) AS open,
        CAST(last_value(price) OVER win AS REAL) AS close
    FROM trade
    WHERE
        created BETWEEN datetime(created, '-1 day') AND datetime()
        {where}
    WINDOW win as (
        -- ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        PARTITION BY market_id
    )
) AS t
    ON m.id = t.market_id
WHERE
    1
    {where}
GROUP BY
    m.id
