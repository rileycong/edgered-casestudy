with raw_clients as (
    select * from {{ source('raw_data', 'clients') }}
)

select 
    client_id,
    entity_type,
    entity_year_established
from
    raw_clients
