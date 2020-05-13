-- Generate order book
select
    side,
    price,
    amount AS amount,
    sum(amount) over (rows unbounded preceding) as total
from (
    select
        side,
        price,
        CAST(sum(balance) AS INT) as amount
    from "order"
    where
        status in ('open','partial')
        and side = 'sell'
        and market_id = ?
    group by
        side, price
    order by
        price asc
)
union all
select
    side,
    price,
    amount,
    sum(amount) over (rows unbounded preceding) as total
from (
    select
        side,
        price,
        CAST(sum(balance) AS INT) as amount
    from "order"
    where
        status in ('open','partial')
        and side = 'buy'
        and market_id = ?
    group by
        side, price
    order by
        price desc
)
order by price asc

