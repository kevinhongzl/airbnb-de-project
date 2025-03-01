with
    today as (
        select listing_id, date, available
        from {{ ref('int_past_available') }}
        where date >= today_date - 365 and date < today_date
    ),
    yesterday as (
        select listing_id, date + 1 as tomorrow_date, available
        from {{ ref('int_past_available') }}
        where date + 1 >= today_date - 365 and date + 1 < today_date
    ),
    tomorrow as (
        select listing_id, date - 1 as yesterday_date, available
        from {{ ref('int_past_available') }}
        where date - 1 >= today_date - 365 and date - 1 < today_date
    ),
    the_day_after_tomorrow as (
        select listing_id, date - 2 as before_yesterday_date, available
        from {{ ref('int_past_available') }}
        where date - 2 >= today_date - 365 and date - 2 < today_date
    ),
    four_night_availibility as (
        select
            today.listing_id,
            today.date as date,
            today.available as today_avail,
            yesterday.available as yest_avail,
            tomorrow.available as tomo_avail,
            the_day_after_tomorrow.available as after_tomo_avail,
        from today
        left join
            yesterday
            on today.date = yesterday.tomorrow_date
            and today.listing_id = yesterday.listing_id
        left join
            tomorrow
            on today.date = tomorrow.yesterday_date
            and today.listing_id = tomorrow.listing_id
        left join
            the_day_after_tomorrow
            on today.date = the_day_after_tomorrow.before_yesterday_date
            and today.listing_id = the_day_after_tomorrow.listing_id
        order by date asc
    ),
    streak_table as (
        select
            listing_id,
            safe_cast(
                yest_avail = true
                and today_avail = true
                and tomo_avail = false
                and after_tomo_avail = false as int64
            ) as streak
        from four_night_availibility
    ),
    result as (
        select listing_id, sum(streak) as num_streak_past_365
        from streak_table
        group by listing_id
    )

select listing_id, num_streak_past_365
from result

union all

select distinct listing_id, null as num_streak_past_365
from {{ ref('fct_calendar_info') }}
where listing_id not in (select listing_id from result)
