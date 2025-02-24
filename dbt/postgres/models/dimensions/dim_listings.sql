-- depends_on: {{ ref('dim_neighbourhoods') }}
{{
  config(
    unique_key = 'listing_id'
  )
}}

with
    listings as (
        select
            row_number() over (partition by id) as row_num,
            id,
            host_id,
            listing_url,
            name,
            description,
            neighborhood_overview,
            picture_url,
            latitude,
            longitude,
            property_type,
            room_type,
            accommodates,
            bathrooms,
            bathrooms_text,
            bedrooms,
            beds,
            amenities,
            neighbourhood_cleansed,
            neighbourhood
        from stg_listings
    )

select
    id as listing_id,
    host_id,
    listing_url,
    lst.name as name,
    description,
    neighborhood_overview,
    picture_url,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms,
    bathrooms_text,
    bedrooms,
    beds,
    amenities,
    ngh.neighbourhood_id as neighbourhood_id,
    lst.neighbourhood_cleansed as neighbourhood_name,
    lst.neighbourhood as neighbourhood_detail
from (select * from listings where row_num = 1) as lst
inner join
    {{ ref('dim_neighbourhoods') }} as ngh on lst.neighbourhood_cleansed = ngh.name
