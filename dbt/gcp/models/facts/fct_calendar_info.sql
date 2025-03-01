{{
  config(
    unique_key = ['listing_id', 'date'],
    cluster_by = ['listing_id', 'date'],
    post_hook = [
      "{{ bq_foreign_key('listing_id', 'dim_listings') }}"
      "{{ bq_foreign_key('scrape_id', 'dim_scrape') }}",
    ]
  )
}}


select
    listing_id,
    date,
    available,
    price,
    adjusted_price,
    minimum_nights,
    maximum_nights,
    (
        select scrape_id from {{ ref('dim_scrape') }} order by last_scraped desc limit 1
    ) as scrape_id,
from {{ ref('stg_calendar') }}
