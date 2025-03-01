with
    quarter_at_least_one_night_avail as (
        select listing_id, quarter, sum(available) as num_available_nights_in_quarter
        from {{ ref('int_month_quarter_available') }}
        group by listing_id, quarter
        having num_available_nights_in_quarter > 1
    ),
    result as (
        select listing_id, count(*) as quarters_w_availability
        from quarter_at_least_one_night_avail
        group by listing_id
    )

select listing_id, quarters_w_availability
from result

union all

select distinct listing_id, 0 as quarters_w_availability
from {{ ref('fct_calendar_info') }}
where listing_id not in (select listing_id from result)
