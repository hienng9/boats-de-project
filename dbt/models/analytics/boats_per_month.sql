with source as (
  select 
  d.year_month,
  count(l.list_id) as number_of_boats
  from {{ ref('fct_listings')}} l
  join {{ ref('dim_date')}} d
  on d.id = l.date_id
  group by d.year_month
)

select * from source