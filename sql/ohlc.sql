-- Open, High, Low, Close by time
-- This query is assembled dynamically.
-- This is the bulk of it though.


-- Generate datetime range
WITH RECURSIVE dtrange(period) AS (
  -- VALUES(strftime('%Y-%m-%d %H:00:00', datetime('now', '-48 hours')))
  VALUES({start})

  UNION ALL
  SELECT datetime(period, '{step}')
  FROM dtrange
  WHERE period < {end}

),

ohlc AS (
    SELECT
        DISTINCT iv.time AS time,
        first_value(iv.price) over w AS open,
        MAX(iv.price) over w AS high,
        MIN(iv.price) over w AS low,
        last_value(iv.price) over w AS close,
        SUM(iv.amount) over w AS volume
    FROM (
        SELECT
            {interval} AS time,
            amount,
            price
        FROM trade
        WHERE market_id = ?
        -- AND created BETWEEN
    ) AS iv
    window w AS (partition BY iv.time)
)

SELECT
    CAST(strftime('%s',period) AS INT) as time,
    COALESCE(ohlc.open,0) as open,
    COALESCE(ohlc.high,0) as high,
    COALESCE(ohlc.low,0) as low,
    COALESCE(ohlc.close,0) as close,
    COALESCE(ohlc.volume,0) as volume,
    COALESCE(ohlc.volume,0) as value
FROM dtrange
LEFT JOIN ohlc
    ON dtrange.period = ohlc.time

