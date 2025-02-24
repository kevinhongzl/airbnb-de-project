{{
  config(
    unique_key = 'name'
  )
}}

with
    neighbourhoods as (
        select neighbourhood as name, neighbourhood_group, geometry
        from stg_neighbourhoods
        order by name
    )

select cast(row_number() over (order by name) as int) as neighbourhood_id, *
from neighbourhoods
