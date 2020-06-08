WITH value AS (
    SELECT
        m.id AS market_id,
        m.asset1,
        m.asset2,
        MAX(t.price) AS price
    FROM market AS m
    JOIN trade AS t
        ON m.id = t.market_id
    GROUP BY m.id
),
reserve AS (
    SELECT
        o.account_id,
        CASE WHEN o.side = 'sell'
             THEN m.asset1
             ELSE m.asset2
        END AS asset_id,
        CASE WHEN o.side = 'sell'
             THEN o.balance
             ELSE o.balance * o.price
        END AS amount
    FROM "order" AS o
    JOIN market AS m
        ON o.market_id = m.id
        AND o.status in ('partial','open')
    WHERE
        o.account_id = :account_id
)

SELECT
    a.id AS asset_id,
    a.symbol,
    a.name,
    a.icon,
    a.scale,
    COALESCE(SUM(l.amount),0) AS balance,
    COALESCE(r.amount,0) AS reserve,
    COALESCE(value.price,0) AS last_price,
    COALESCE(SUM(l.amount) * value.price,0) AS usd_value,
    MIN(l.created) AS opening,
    MAX(l.created) AS ending
FROM asset AS a
LEFT JOIN ledger AS l
    ON a.id = l.asset_id AND l.account_id = :account_id
LEFT JOIN value
    ON value.asset1 = a.id AND value.asset2 = 1 -- USD Asset
LEFT JOIN reserve AS r
    ON r.account_id = l.account_id AND a.id = r.asset_id
GROUP BY
    a.id
ORDER BY
    balance DESC
