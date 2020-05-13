-- Generate order book
select
    side,
    price,
    amount AS amount,
    sum(amount) over (rows unbounded preceding) as total
from (
    select
        direction as side,
        price,
        CAST(sum(amount_left) AS INT) as amount
    from "order"
    where
        status in ('open','partial')
        and direction = 'sell'
        and market_id = ?
    group by
        direction, price
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
        direction as side,
        price,
        CAST(sum(amount_left) AS INT) as amount
    from "order"
    where
        status in ('open','partial')
        and direction = 'buy'
        and market_id = ?
    group by
        direction, price
    order by
        price desc
)
order by price asc

