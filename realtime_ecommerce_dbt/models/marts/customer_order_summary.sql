{{ config(
    materialized='table'
) }}

SELECT
    customer_unique_id,

    COUNT(DISTINCT order_id) AS total_orders,

    SUM(order_total_value) AS total_revenue,

    AVG(order_total_value) AS average_order_value,

    SUM(total_items) AS total_items,

    SUM(
        CASE
            WHEN order_status = 'delivered' THEN 1
            ELSE 0
        END
    ) AS delivered_orders,

    SUM(
        CASE
            WHEN order_status = 'canceled' THEN 1
            ELSE 0
        END
    ) AS cancelled_orders,

    AVG(delivery_time_days) AS average_delivery_days,

    AVG(delivery_delay_days) AS average_delivery_delay_days

FROM {{ ref('fct_orders') }}

GROUP BY customer_unique_id
