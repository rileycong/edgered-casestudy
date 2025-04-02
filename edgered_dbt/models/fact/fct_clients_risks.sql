with aggregated_data as (
    select
        client_id,
        sum(case when payment_code != 'DEFAULT' then payment_amt else 0 end) as total_amount_paid,
        avg(case when payment_code != 'DEFAULT' then payment_amt else null end) as avg_amount_per_transaction,
        count(case when payment_code != 'DEFAULT' then transaction_id else null end) as num_payments,
        count(distinct contract_id) as num_contracts,
        count(case when payment_code = 'DEFAULT' then transaction_id else null end) as num_defaults,
        sum(case when payment_code = 'DEFAULT' then payment_amt else 0 end) as total_defaulted_amount
    from {{ ref('src_payments') }}
    group by client_id
),

risk_factors as (
    select
        client_id,
        case
            when entity_type = 'Australian Private Company' and EXTRACT(MONTH from transaction_datetime) = 7 then 1
            when entity_type = 'Australian Public Company' and EXTRACT(MONTH from transaction_datetime) = 2 then 1
            when entity_type = 'Discretionary Investment Trust' and EXTRACT(MONTH from transaction_datetime) in (3, 7) then 1
            when entity_type = 'Discretionary Trading Trust' and EXTRACT(MONTH from transaction_datetime) = 7 then 1
            when entity_type = 'Family Partnership' and EXTRACT(MONTH from transaction_datetime) = 7 then 1
            when entity_type = 'Hybrid Trust' and EXTRACT(MONTH from transaction_datetime) = 6 then 1
            when entity_type = 'Individual/Sole Trader' and EXTRACT(MONTH from transaction_datetime) in (4, 5, 7) then 1
            when entity_type = 'Other Partnership' and EXTRACT(MONTH from transaction_datetime) in (8, 10, 11) then 1
            else 0
        end as in_risky_month,

        case
            when entity_type = 'Australian Private Company' and entity_age in (10, 12, 17) then 1
            when entity_type = 'Australian Public Company' and entity_age in (12, 13, 16) then 1
            when entity_type = 'Discretionary Investment Trust' and entity_age = 10 then 1
            when entity_type = 'Discretionary Trading Trust' and entity_age = 12 then 1
            when entity_type = 'Family Partnership' and entity_age in (14, 16) then 1
            when entity_type = 'Hybrid Trust' and entity_age = 18 then 1
            when entity_type = 'Individual/Sole Trader' and entity_age in (10, 11, 16, 17, 18) then 1
            when entity_type = 'Other Partnership' and entity_age = 14 then 1
            else 0
        end as in_risky_age,

        case
            when entity_type = 'Australian Private Company' then 4
            when entity_type = 'Australian Public Company' then 2
            when entity_type = 'Discretionary Investment Trust' then 4
            when entity_type = 'Discretionary Trading Trust' then 3
            when entity_type = 'Family Partnership' then 2
            when entity_type = 'Hybrid Trust' then 1
            when entity_type = 'Individual/Sole Trader' then 3
            when entity_type = 'Other Partnership' then 1   
            else 0
        end as risky_business_types
        
    from {{ ref('dim_payments_w_clients') }}
),

final_risk_score as (
    select
        ad.client_id,
        ad.total_amount_paid,
        ad.avg_amount_per_transaction,
        ad.num_payments,
        ad.num_contracts,
        ad.num_defaults,
        ad.total_defaulted_amount,

        -- Log transformation for scaling
        log(ad.total_amount_paid + 1) as total_amount_paid_scaled,
        log(ad.avg_amount_per_transaction + 1) as avg_transaction_scaled,
        log(ad.total_defaulted_amount + 1) as total_defaulted_amount_scaled,

        -- Calculate the risk score based on different factors
        (ad.num_defaults * 5) +
        (ad.total_defaulted_amount / 1000) +
        (8 - log(ad.total_amount_paid + 1)) +
        (10 - log(ad.avg_amount_per_transaction + 1)) +
        (20 - log(ad.total_defaulted_amount + 1)) +
        (rf.in_risky_month * 5) +
        (rf.in_risky_age * 5) +
        (5 * rf.risky_business_types) as risk_score
    from aggregated_data ad
    join risk_factors rf
        on ad.client_id = rf.client_id
),

quantiles as (
    select 
        APPROX_QUANTILES(risk_score, 100) AS q
    from final_risk_score
)

-- Final output with risk level classification
select
    client_id,
    total_amount_paid,
    avg_amount_per_transaction,
    num_payments,
    num_contracts,
    num_defaults,
    total_defaulted_amount,
    risk_score,
    -- Classify the clients into risk levels based on the risk score
    case
        when risk_score <= (SELECT q[OFFSET(35)] FROM quantiles) then 'Low'
        when risk_score <= (SELECT q[OFFSET(75)] FROM quantiles) then 'Medium'
        when risk_score <= (SELECT q[OFFSET(90)] FROM quantiles) then 'High'
        else 'Critical'
    end as risk_level
    
from final_risk_score
order by risk_score desc
