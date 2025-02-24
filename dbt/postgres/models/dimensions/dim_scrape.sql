{{
  config(
    unique_key = 'scrape_id'
  )
}}

select scrape_id, last_scraped, source
from stg_listings
order by scrape_id desc
limit 1
