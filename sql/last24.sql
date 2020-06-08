-- Open, High, Low, Close over last 24h
SELECT
    m.id AS market_id,
    m.code,
    m.name,
    COALESCE(open,0) AS open,
    COALESCE(MAX(price),0) AS high,
    COALESCE(MIN(price),0) AS low,
    COALESCE(close,0) AS close,
    COALESCE(SUM(amount),0) AS volume,
    COALESCE(AVG(price),0) AS avg_price,
    COALESCE((close - open) / open,0) AS change
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
        {sub_where}
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
