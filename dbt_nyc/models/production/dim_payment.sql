{{ config(materialized = 'table') }}

with payment_staging as (
    select distinct 
        payment_type_id
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
)

select 
    {{ dbt_utils.surrogate_key(['payment_type_id']) }} as payment_type_key,
    payment_type_id,
    {{ get_payment_description('payment_type_id') }} as payment_description
from 
    payment_staging
where
    payment_type_id is not null
order by
    payment_type_id asc 