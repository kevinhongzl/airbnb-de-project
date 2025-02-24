{{
  config(
    unique_key = 'reviewer_id',
    post_hook = [
      "{{ bq_primary_key('reviewer_id') }}"
    ]
  )
}}

select reviewer_id, reviewer_name as name
from {{ ref('stg_reviews') }}
qualify row_number() over (partition by reviewer_id) = 1
