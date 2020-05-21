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
)

SELECT
    a.id AS asset_id,
    a.symbol,
    a.name,
    a.icon,
    SUM(l.amount) AS balance,
    value.price AS last_price,
    SUM(l.amount) * value.price AS usd_value,
    MIN(l.created) AS opening,
    MAX(l.created) AS ending
FROM asset AS a
LEFT JOIN ledger AS l
    ON a.id = l.asset_id AND l.owner_id = ?
LEFT JOIN value
    ON value.asset1 = a.id AND value.asset2 = 1 -- USD Asset
-- WHERE
--    owner_id = 1
GROUP BY
    a.id
ORDER BY
    balance DESC
