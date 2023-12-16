with source as (
  select
    distinct 
    location_region_code,
    location_area_code,
    location_region_label,
    location_area_label
  from {{ source('boats', 'raw_boats')}}
)

select 
  md5 ( concat (location_region_code, location_area_code) ) as location_id,
  location_region_label as region_name,
  location_area_label as area_name,
  current_timestamp() as Insertion_Timestamp 
from source
order by location_id