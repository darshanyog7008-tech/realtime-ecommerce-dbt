{{ config(
    materialized='view'
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

    customer_zip_code_prefix,
    customer_city,
    customer_state,

    total_items,
    unique_products,
    unique_sellers,

    items_revenue,
    total_freight,
    average_item_price,
    minimum_item_price,
    maximum_item_price,

    payment_count,
    payment_type_count,
    total_payment_value,
    max_payment_installments,

    order_item_value,
    order_total_value,

    approval_time_hours,
    delivery_time_days,
    delivery_delay_days,

    delivery_performance,
    order_value_bucket,
    order_size_bucket,
    payment_status

FROM {{ source('realtime_ecommerce', 'gold_order_fact') }}
