with source as (
  select
    distinct 
    ad_details_boat_engine_brand_single_code,
    ad_details_boat_engine_brand_single_label
  from {{ source('boats', 'raw_boats')}}
  where ad_details_boat_engine_brand_single_code is not null
  --order by ad_details_boat_engine_brand_single_code
)

select 
  ad_details_boat_engine_brand_single_code as engine_id,
  ad_details_boat_engine_brand_single_label as engine_brand,
  current_timestamp() as Insertion_Timestamp 
from source
order by engine_id