with payments as (
    select * from {{ ref('stg_payments') }} 
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (select 
    order_id,
    customer_id,
    SUM(amount) as amount
from orders left join payments using (order_id)
where payments.status like 'success'
group by order_id, customer_id
order by order_id asc
)

select * from final