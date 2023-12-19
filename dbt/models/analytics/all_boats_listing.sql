with source as (
  select 
    list_id,
    listing_title,
    date_id,
    list_hour,
    price,
    region_name,
    area_name,
    engine_brand,
    engine_power,
    engine_hp,
    fuel,
    general_condition,
    registration_year,
    store
  from {{ ref('fct_listings') }} fl 
  left join {{ ref('dim_location') }} l 
  on l.location_id = fl.location_id
  left join {{ ref('dim_engine') }} e 
  on e.engine_id = fl.engine_id
  left join {{ ref('dim_fuel') }} fu 
  on fu.fuel_id = fl.fuel_id
  left join {{ ref('dim_store') }} s 
  on s.store_id = fl.store_id
)

select * from source