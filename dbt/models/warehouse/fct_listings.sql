with source as (
  select 
    list_id,
    subject as listing_title,
    format_date('%F', list_date) as date_id,
    list_hour,
    list_price_value as price,
    md5 ( concat (location_region_code, location_area_code) ) as location_id,
    ad_details_boat_engine_brand_single_code as engine_id,
    ad_details_cx_engine_power_single_code as engine_power,
    EngineHp as engine_hp,
    ad_details_fuel_single_code as fuel_id,
    ad_details_general_condition_single_code as general_condition,
    ad_details_regdate_single_code as registration_year,
    store_details_id as store_id,
    ad_details_cx_used_hour_single_code as used_hours
  from {{ source('boats', 'raw_boats')}}
)

select *,
  current_timestamp() as Insertion_Timestamp 
from source