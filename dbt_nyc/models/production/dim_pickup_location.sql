{{ config(materialized = 'table') }}

with pickup_location as (
    select distinct 
        pickup_location_id,
        pickup_latitude,
        pickup_longitude
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
)

select 
    *
from 
    pickup_location
where
    pickup_location_id is not null
order by 
    pickup_location_id asc