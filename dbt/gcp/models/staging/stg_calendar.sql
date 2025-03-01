select
    safe_cast(listing_id as int64) as listing_id,
    safe_cast(date as date) as date,
    (case available when 't' then true when 'f' then false end) as available,
    safe_cast(regexp_replace(price, "[$,]", "") as float64) as price,
    safe_cast(adjusted_price as float64) as adjusted_price,
    safe_cast(minimum_nights as int64) as minimum_nights,
    safe_cast(maximum_nights as int64) as maximum_nights
from {{ source("dataset", "raw_calendar") }}
order by listing_id asc, date asc
