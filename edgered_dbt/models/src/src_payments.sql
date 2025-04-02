with raw_clients as (
    select * from {{ source('raw_data', 'raw_payments') }}
)

select 
    transaction_id,
    contract_id,
    client_id,
    payment_amt,
    payment_code,
    transaction_datetime
from
    raw_clients
