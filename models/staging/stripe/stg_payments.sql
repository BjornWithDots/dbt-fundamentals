SELECT 
        id AS payment_id,
        orderid as order_id,
        PaymentMethod as payment_method,
        Status AS payment_status,
        {{cents_to_dollars('amount',4)}} as amount,
        Created as created_at,
        max(created) over (partition by orderid) as payment_finalized_date,
        sum({{cents_to_dollars('amount',4)}}) over (partition by orderid) as total_amount_paid
FROM {{source('stripe','stripe_payment')}}
{{ limit_dev_data('created',100) }}
AND status != 'fail'