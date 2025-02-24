{{
  config(
    unique_key = ['scrape_id', 'listing_id'],
    cluster_by = ['scrape_id', 'listing_id'],
    post_hook = [
      "{{ bq_foreign_key('scrape_id', 'dim_scrape') }}",
      "{{ bq_foreign_key('listing_id', 'dim_listings') }}"
    ]
  )
}}

select
    id as listing_id,
    scrape_id,
    price,
    minimum_nights,
    maximum_nights,
    minimum_minimum_nights,
    maximum_minimum_nights,
    minimum_maximum_nights,
    maximum_maximum_nights,
    minimum_nights_avg_ntm,
    maximum_nights_avg_ntm,
    calendar_updated,
    has_availability,
    availability_30,
    availability_60,
    availability_90,
    availability_365,
    calendar_last_scraped,
    number_of_reviews,
    number_of_reviews_ltm,
    number_of_reviews_l30d,
    first_review,
    last_review,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    license,
    instant_bookable,
    reviews_per_month
from {{ ref('stg_listings') }}
