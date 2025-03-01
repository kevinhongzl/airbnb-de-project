with
    result as (
        select listing_id, sum(safe_cast(available as int64)) as num_available_past_365,
        from {{ ref('int_past_available') }}
        where date >= today_date - 365 and date < today_date
        group by listing_id, today_date
    )

select listing_id, num_available_past_365
from result

union all

select distinct listing_id, null as num_available_past_365
from {{ ref('fct_calendar_info') }}
where listing_id not in (select listing_id from result)
