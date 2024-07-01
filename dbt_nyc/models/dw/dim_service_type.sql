{{ config(materialized = 'table') }}

with service_type_staging as (
    select distinct
        service_type
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
)

select 
    service_type as service_type_id,
    {{ get_service_name('service_type') }} as service_name
from 
    service_type_staging
where 
    service_type is not null
order by
    service_type asc