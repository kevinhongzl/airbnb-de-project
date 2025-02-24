{{
  config(
    unique_key = 'host_id',
    post_hook = [
      "{{ bq_primary_key('host_id') }}",
      "{{ bq_foreign_key('neighbourhood_id', 'dim_neighbourhoods') }}"
    ]
  )
}}

select
    h.host_id,
    h.url,
    h.name,
    h.since,
    h.location,
    h.about,
    h.thumbnail_url,
    h.picture_url,
    d_nbh.neighbourhood_id,
    h.neighbourhood_name
from
    (
        select
            host_id,
            host_url as url,
            host_name as name,
            host_since as since,
            host_location as location,
            host_about as about,
            host_thumbnail_url as thumbnail_url,
            host_picture_url as picture_url,
            host_neighbourhood as neighbourhood_name
        from {{ ref('stg_listings') }}
        qualify row_number() over (partition by host_id) = 1
    ) as h
inner join
    {{ ref('seed_host_neighbourhood_cleansed') }} as h_nbh_c
    on h.neighbourhood_name = h_nbh_c.host_neighbourhood
inner join
    {{ ref('dim_neighbourhoods') }} as d_nbh
    on h_nbh_c.neighbourhood_cleansed = d_nbh.name
