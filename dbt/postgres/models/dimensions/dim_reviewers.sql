{{
  config(
    unique_key = 'reviewer_id'
  )
}}

with
    reviewers as (
        select
            row_number() over (partition by reviewer_id) as row_num,
            reviewer_id,
            reviewer_name
        from stg_reviews
    )

select reviewer_id, reviewer_name as name
from reviewers
where row_num = 1
