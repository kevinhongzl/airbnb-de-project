select
    listing_id,
    extract(month from date) as month,
    extract(quarter from date) as quarter,
    safe_cast(available as int64) as available
from {{ ref('int_past_available') }}
where date >= today_date - 365 and date < today_date
order by listing_id asc, date asc
