{{
  config(
    unique_key = ['scrape_id', 'host_id'],
    cluster_by = ['scrape_id', 'host_id'],
    post_hook = [
      "{{ bq_foreign_key('scrape_id', 'dim_scrape') }}",
      "{{ bq_foreign_key('host_id', 'dim_hosts') }}"
    ]
  )
}}

select
    host_id,
    scrape_id,
    host_response_time as response_time,
    host_response_rate as response_rate,
    host_acceptance_rate as acceptance_rate,
    host_is_superhost as is_superhost,
    host_listings_count as listings_count,
    host_total_listings_count as total_listings_count,
    host_verifications as verifications,
    host_has_profile_pic as has_profile_pic,
    host_identity_verified as identity_verified,
    calculated_host_listings_count as calculated_listings,
    calculated_host_listings_count_entire_homes as calculated_entire_homes,
    calculated_host_listings_count_private_rooms as calculated_private_rooms,
    calculated_host_listings_count_shared_rooms as calculated_shared_rooms
from {{ ref('stg_listings') }}
qualify row_number() over (partition by host_id) = 1
