-- depends_on: {{ ref('dim_neighbourhoods') }}
{{
  config(
    unique_key = 'listing_id',
    post_hook = [
      "{{ bq_primary_key('listing_id') }}",
      "{{ bq_foreign_key('host_id', 'dim_hosts') }}",
      "{{ bq_foreign_key('neighbourhood_id', 'dim_neighbourhoods') }}"
    ]
  )
}}

with
    listings as (
        select
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
        from {{ ref('stg_listings') }}
        qualify row_number() over (partition by id) = 1
    )

select
    id as listing_id,
    host_id,
    listing_url,
    lst.name,
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
    ngh.neighbourhood_id,
    lst.neighbourhood as neighbourhood_detail
from listings as lst
inner join
    {{ ref('dim_neighbourhoods') }} as ngh on lst.neighbourhood_cleansed = ngh.name
