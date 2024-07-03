{{ config(materialized = 'table') }}

with rate_code_staging as (
    select distinct 
        rate_code_id
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
)

select 
    {{ dbt_utils.surrogate_key(['rate_code_id']) }} as rate_code_key,
    rate_code_id,
    {{ get_rate_code_description('rate_code_id') }} as rate_code_description
from 
    rate_code_staging
where 
    rate_code_id is not null
    and 
    cast(rate_code_id as integer) < 7
order by
    rate_code_id asc