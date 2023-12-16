with source as (
  select distinct store_details_id, store_details_name
  from {{ source('boats', 'raw_boats')}}
  where ad_details_fuel_single_code is not null
)

select 
  store_details_id as store_id,
  store_details_name as store,
  current_timestamp() as Insertion_Timestamp
from source
order by store_id