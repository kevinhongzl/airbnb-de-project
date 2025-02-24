{{
  config(
    unique_key = 'name',
    post_hook = [
      "{{ bq_primary_key('neighbourhood_id') }}"
    ]
  )
}}

with
    neighbourhoods as (
        select neighbourhood as name, neighbourhood_group, geometry
        from {{ ref('stg_neighbourhoods') }}
        order by name
    )

select
    cast(row_number() over (order by name) as int) as neighbourhood_id,
    name,
    english as english_name,
    neighbourhood_group,
    geometry
from neighbourhoods
inner join
    {{ ref('seed_neighbourhood_english') }}
    on neighbourhoods.name = seed_neighbourhood_english.chinese
