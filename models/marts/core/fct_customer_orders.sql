with orders as
(
    select * from {{ ref('stg_orders') }}
),

customers as
(
    select * from {{ ref('stg_customers') }}
),

payment as
(
    select * from {{ ref('stg_payments') }}
),

paid_orders as (
    select orders.order_id,
    orders.customer_id,
    orders.order_date as order_placed_at,
    orders.order_status,
    payment.total_amount_paid,
    payment.payment_finalized_date,
    customers.customer_first_name,
    customers.customer_last_name
from orders 
left join  payment
    on orders.order_id = payment.order_id
left join customers 
    on orders.customer_id = customers.customer_id 
),

customer_orders 
as (select customers.customer_id
    , min(orders.order_date) as first_order_date
    , max(orders.order_date) as most_recent_order_date
    , count(orders.order_id) as number_of_orders
from customers 
left join orders
    on orders.customer_id = customers.customer_id 
group by 1)

select
p.*,
row_number() over (order by p.order_id) as transaction_seq,
row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
case 
    when c.first_order_date = p.order_placed_at then 'new'
    else 'return' 
end as nvsr,
x.clv_bad as customer_lifetime_value,
c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join 
(
        select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id
order by order_id