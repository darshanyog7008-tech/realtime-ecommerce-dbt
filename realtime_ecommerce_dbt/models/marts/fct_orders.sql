{{ config(
    materialized='table'
) }}

SELECT
    order_id,
    customer_id,
    customer_unique_id,
    order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    customer_city,
    customer_state,

    total_items,
    unique_products,
    unique_sellers,

    items_revenue,
    total_freight,
    total_payment_value,

    order_item_value,
    order_total_value,

    approval_time_hours,
    delivery_time_days,
    delivery_delay_days,

    delivery_performance,
    order_value_bucket,
    order_size_bucket,
    payment_status

FROM {{ ref('stg_orders') }}
