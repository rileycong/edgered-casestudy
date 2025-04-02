with clients_payments_w_history as (
    select
        p.transaction_id,
        p.contract_id,
        p.client_id,
        p.payment_amt,
        -- Calculate the total amount paid before this transaction
        sum(case when p2.transaction_id < p.transaction_id and p2.payment_code != 'DEFAULT' then p2.payment_amt else 0 end) 
            over (partition by p.client_id order by p.transaction_id) as total_amount_paid_before,
        
        -- Calculate the average amount per transaction before this transaction
        avg(case when p2.transaction_id < p.transaction_id and p2.payment_code != 'DEFAULT' then p2.payment_amt else null end)
            over (partition by p.client_id order by p.transaction_id) as avg_amount_per_transaction_before,
        
        -- Calculate the number of payments before this transaction
        count(case when p2.transaction_id < p.transaction_id and p2.payment_code != 'DEFAULT' then 1 else null end) 
            over (partition by p.client_id order by p.transaction_id) as num_payments_before,
        
        -- Calculate the number of contracts before this transaction
        (select count(distinct p2.contract_id)
            from {{ ref('src_payments') }} p2
            where p2.client_id = p.client_id
            and p2.transaction_id < p.transaction_id) as num_contracts_before,            

        -- Calculate the number of defaults before this transaction
        count(case when p2.transaction_id < p.transaction_id and p2.payment_code = 'DEFAULT' then 1 else null end) 
            over (partition by p.client_id order by p.transaction_id) as num_defaults_before

    from {{ ref('src_payments') }} p
    left join {{ ref('src_payments') }} p2 
        on p.client_id = p2.client_id
)

select

    {{ dbt_utils.generate_surrogate_key(['transaction_id', 'contract_id', 'client_id']) }} as clients_payments_id,
    transaction_id,
    contract_id,
    client_id,
    payment_amt,
    total_amount_paid_before,
    avg_amount_per_transaction_before,
    num_payments_before,
    num_contracts_before,
    num_defaults_before

from clients_payments_w_history
