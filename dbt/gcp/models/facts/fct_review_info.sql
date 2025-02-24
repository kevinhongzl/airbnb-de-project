{{
  config(
    unique_key = 'review_id',
    merge_exclude_columns = ['scrape_id'],
    cluster_by = ["listing_id", "reviewer_id"],
    post_hook = [
      "{{ bq_primary_key('review_id') }}",
      "{{ bq_foreign_key('listing_id', 'dim_listings') }}",
      "{{ bq_foreign_key('reviewer_id', 'dim_reviewers') }}",
      "{{ bq_foreign_key('scrape_id', 'dim_scrape') }}"
    ]
  )
}}

select
    id as review_id,
    listing_id,
    reviewer_id,
    (
        select scrape_id from {{ ref('dim_scrape') }} order by last_scraped desc limit 1
    ) as scrape_id,
    date,
    comments
from {{ ref('stg_reviews') }}
