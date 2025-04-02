select
    transaction_id,
    contract_id,
    client_id,
    transaction_datetime,
    payment_code
from
    {{ ref('src_payments') }}
