{{ config(materialized = 'table') }}

with vendor_staging as (
    select distinct
        vendor_id
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
)

select 
    {{ dbt_utils.surrogate_key(['vendor_id']) }} as vendor_key,
    vendor_id,
    {{ get_vendor_description('vendor_id') }} as vendor_name
from 
    vendor_staging
where
    vendor_id is not null
    and
    cast(vendor_id as integer) < 3
order by
    vendor_id asc