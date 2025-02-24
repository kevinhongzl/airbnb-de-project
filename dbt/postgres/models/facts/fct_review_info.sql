{{
  config(
    unique_key = 'review_id'
  )
}}

with
    scrape as (
        select '1' as join_helper, scrape_id
        from dim_scrape
        order by last_scraped desc
        limit 1
    ),

    reviews as (
        select
            '1' as join_helper, id as review_id, listing_id, reviewer_id, date, comments
        from stg_reviews
    )

select review_id, listing_id, reviewer_id, scrape_id, date, comments
from reviews
inner join scrape on reviews.join_helper = scrape.join_helper
