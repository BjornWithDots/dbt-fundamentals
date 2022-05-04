SELECT 
        id AS payment_id,
        orderid as order_id,
        PaymentMethod as payment_method,
        Status AS payment_status,
        {{cents_to_dollars('amount',4)}} as amount,
        Created as created_at
FROM {{source('stripe','stripe_payment')}}
{{ limit_dev_data('created',100) }}