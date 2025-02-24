{{
  config(
    unique_key = 'host_id'
  )
}}

with
    hosts as (
        select
            row_number() over (partition by host_id) as row_num,
            host_id,
            host_url,
            host_name,
            host_since,
            host_location,
            host_about,
            host_thumbnail_url,
            host_picture_url,
            host_neighbourhood
        from stg_listings
    )

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
from hosts
where row_num = 1
