{{
    config(
        materialized='table'    
    )
}}

WITH
p AS (
    SELECT * FROM {{ ref('dim_payments') }}
),
c AS (
    SELECT * FROM {{ ref('dim_clients') }}
)

SELECT
    p.transaction_id,
    p.contract_id,
    p.client_id,
    p.transaction_datetime,
    c.entity_type,
    c.entity_age,
    p.payment_code
FROM p
LEFT JOIN c ON p.client_id = c.client_id