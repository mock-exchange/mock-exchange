SELECT
    b.ntile,
    SUM(b.amount)::bigint AS amount
FROM (
    SELECT
        l.account_id,
        SUM(l.amount)::bigint AS amount,
        NTILE(100) OVER (
            ORDER BY SUM(l.amount)
        ) AS ntile
    FROM ledger AS l
    WHERE
        l.asset_id = 1
        AND l.amount > 0
    GROUP BY
        l.account_id,
        l.asset_id
    ORDER BY
        SUM(l.amount) DESC
) AS b
GROUP BY
    b.ntile
ORDER BY
    b.ntile DESC
