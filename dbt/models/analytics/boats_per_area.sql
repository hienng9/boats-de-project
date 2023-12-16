with source as (
  select 
  loc.region_name,
  loc.area_name,
  count(l.list_id) as number_of_boats
  from {{ ref('fct_listings')}} l
  -- join {{ ref('dim_date')}} d
  -- on d.id = l.date_id
  join {{ ref('dim_location') }} loc
  on loc.location_id = l.location_id
  group by loc.region_name, loc.area_name
)

select * from source