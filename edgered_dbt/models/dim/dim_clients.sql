with scr_clients as (
    select * from {{ ref('src_clients') }}
)

select 
    client_id,
    entity_type,
    EXTRACT(YEAR FROM CURRENT_DATE()) - entity_year_established AS entity_age 
from
    scr_clients
