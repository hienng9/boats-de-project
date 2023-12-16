with source as (
  select distinct ad_details_fuel_single_code, ad_details_fuel_single_label
  from {{ source('boats', 'raw_boats')}}
  where ad_details_fuel_single_code is not null
)

select 
  ad_details_fuel_single_code as fuel_id,
  ad_details_fuel_single_label as fuel,
  current_timestamp() as Insertion_Timestamp
from source
order by fuel_id