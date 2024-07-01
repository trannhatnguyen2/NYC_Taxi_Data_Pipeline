{{ config(materialized = 'table') }}

with dropoff_location as (
    select distinct 
        dropoff_location_id,
        dropoff_latitude,
        dropoff_longitude
    from 
        staging.nyc_taxi
    where 
        vendor_id is not null
)

select 
    *
from 
    dropoff_location
where
    dropoff_location_id is not null
order by 
    dropoff_location_id asc