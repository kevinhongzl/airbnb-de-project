{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['listing_id', 'scrape_id']
  )
}}

select
    arh.listing_id as listing_id,
    (select min(date) from {{ ref('stg_calendar') }}) as today_date,
    arh.num_available_past_365 as num_available_past_365,
    sth.num_streak_past_365 as num_streak_past_365,
    smh.max_consecutive_months as max_consecutive_months,
    sqh.quarters_w_availability as quarters_w_availability,
    num_available_past_365 / 365 as availablity_rate,
    (
        case
            when num_available_past_365 = 0
            then 0
            else num_streak_past_365 / num_available_past_365
        end
    ) as streakiness,
    (
        select scrape_id from {{ ref('dim_scrape') }} order by last_scraped desc limit 1
    ) as scrape_id
from {{ ref('int_availablity_rate_helper') }} as arh
inner join {{ ref('int_streakiness_helper') }} as sth on arh.listing_id = sth.listing_id
inner join
    {{ ref('int_seasonality_month_helper') }} as smh on arh.listing_id = smh.listing_id
inner join
    {{ ref('int_seasonality_quarter_helper') }} as sqh
    on arh.listing_id = sqh.listing_id
