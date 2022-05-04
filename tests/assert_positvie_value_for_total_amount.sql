select
    order_id,
    sum(amount) as totalAmount
from {{ ref('stg_payments') }}
group by order_id
having totalAmount < 0