select
    safe_cast(listing_id as int64) as listing_id,
    safe_cast(id as int64) as id,
    safe_cast(date as date) as date,
    safe_cast(reviewer_id as int64) as reviewer_id,
    safe_cast(reviewer_name as string) as reviewer_name,
    safe_cast(comments as string) as comments
from {{ source("dataset", "raw_reviews") }}
