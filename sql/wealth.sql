SELECT
    l.account_id,
    SUM(l.amount)
FROM ledger AS l
GROUP BY
    l.account_id
ORDER BY
    SUM(l.amount) DESC
