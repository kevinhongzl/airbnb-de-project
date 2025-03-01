select
    listing_id,
    date,
    available,
    (select min(date) from {{ ref('stg_calendar') }}) as today_date
from {{ ref('fct_calendar_info') }}
order by listing_id asc, date asc
