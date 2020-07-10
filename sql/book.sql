-- Generate order book
select
    side,
    price::text,
    amount::text AS amount,
    (sum(amount) over (rows unbounded preceding))::text as total
from (
    select
        side,
        price,
        CAST(sum(balance) AS INT) as amount
    from "order"
    where
        status in ('open','partial')
        and side = 'sell'
        and market_id = %(market_id)s
    group by
        side, price
    order by
        price asc
) as sells
union all
select
    side,
    price::text,
    amount::text,
    (sum(amount) over (rows unbounded preceding))::text as total
from (
    select
        side,
        price,
        CAST(sum(balance) AS INT) as amount
    from "order"
    where
        status in ('open','partial')
        and side = 'buy'
        and market_id = %(market_id)s
    group by
        side, price
    order by
        price desc
) as buys
order by side asc, price asc

