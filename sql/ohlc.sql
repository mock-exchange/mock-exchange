-- Open, High, Low, Close by time


WITH RECURSIVE dtrange(period) AS (
  VALUES(strftime('%Y-%m-%d %H:00:00', datetime('now', '-8 hours')))

  UNION ALL
  -- SELECT datetime(period, '+6 hour')
  SELECT datetime(period, '+15 minute')
  FROM dtrange
  WHERE period < datetime('now')

)

SELECT
    CAST(strftime('%s',period) AS INT) as time,
    COALESCE(foo.open,0) as open,
    COALESCE(foo.high,0) as high,
    COALESCE(foo.low,0) as low,
    COALESCE(foo.close,0) as close,
    COALESCE(foo.volume,0) as volume,
    COALESCE(foo.volume,0) as value
FROM dtrange
LEFT JOIN (

SELECT
    DISTINCT bar.time AS time,
    first_value(bar.price) over w AS open,
    MAX(bar.price) over w AS high,
    MIN(bar.price) over w AS low,
    last_value(bar.price) over w AS close,
    SUM(bar.amount) over w AS volume

FROM (
    SELECT

        -- 1m
        -- CAST(strftime('%Y-%m-%d %H:%M', created) AS TEXT) AS time,

        -- 5m
        -- CAST(strftime('%Y-%m-%d %H:', created) AS TEXT) || CAST(printf('%02d', (CAST(strftime('%M',created) AS INT) / 5) * 5) AS TEXT) AS time,

        -- 15m
        CAST(strftime('%Y-%m-%d %H:', created) AS TEXT) || CAST(printf('%02d', (CAST(strftime('%M',created) AS INT) / 15) * 15) AS TEXT) || ':00' AS time,

        -- 1h
        -- CAST(strftime('%Y-%m-%d %H', created) AS TEXT) AS time,

        -- 6h
        -- CAST(strftime('%Y-%m-%d ', created) AS TEXT) || CAST(printf('%02d', (CAST(strftime('%H',created) AS INT) / 6) * 6) AS TEXT) AS time,

        -- 1d
        -- date(created) AS time,

        amount,
        price
    FROM trade
    WHERE market_id = ?
) as bar
window w AS (partition BY bar.time)

) as foo
    ON dtrange.period = foo.time
/*
SELECT
    -- DISTINCT date(created) AS time, -- by 1d
    -- DISTINCT strftime('%Y-%m-%d %H', created) AS time, -- by 1h
    created,
    DISTINCT strftime('%Y-%m-%d %H:%M:', created) || CAST(CAST(strftime('%S',datetime()) AS INT) / 15 * 15 AS TEXT) AS time,
    
    first_value(price) over w AS open,
    MAX(price) over w AS high,
    MIN(price) over w AS low,
    last_value(price) over w AS close,
    CAST(SUM(amount) over w AS INT) AS value
FROM trade
WHERE market_id = 1
window w AS (partition BY strftime('%Y-%m-%d %H', created))
*/

/*
SELECT
    DISTINCT date(created) AS time,
    first_value(price) over w AS open,
    MAX(price) over w AS high,
    MIN(price) over w AS low,
    last_value(price) over w AS close,
    CAST(SUM(amount) over w AS INT) AS value
FROM trade
WHERE market_id = 1
window w AS (partition BY date(created))
*/
