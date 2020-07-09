-- Open, High, Low, Close by time
-- This query is assembled dynamically.
-- This is the bulk of it though.


-- Generate datetime range
WITH dtrange AS (
    SELECT
        period
    FROM generate_series(
        '{start}'::timestamp,
        '{end}'::timestamp,
        '{interval}'
    ) AS period
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
            {convert} AS time,
            amount,
            price
        FROM trade
        WHERE market_id = %s
        AND created BETWEEN '{start}'::timestamp AND '{end}'::timestamp
    ) AS iv
    window w AS (partition BY iv.time)
)

SELECT
    to_char(period, 'YYYY-MM-DD') || 'T' || to_char(period,'HH:MI:SSZ') as dt,
    extract(epoch from period)::text as time,
    COALESCE(ohlc.open,0)::text as open,
    COALESCE(ohlc.high,0)::text as high,
    COALESCE(ohlc.low,0)::text as low,
    COALESCE(ohlc.close,0)::text as close,
    COALESCE(ohlc.volume,0)::text as volume,
    COALESCE(ohlc.volume,0)::text as value
FROM dtrange
LEFT JOIN ohlc
    ON dtrange.period = ohlc.time

