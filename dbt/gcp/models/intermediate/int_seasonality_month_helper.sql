with
    month_available as (
        select listing_id, month, sum(available) > 0 as available
        from {{ ref('int_month_quarter_available') }}
        group by listing_id, month
        order by listing_id asc, month asc
    ),
    first_table as (
        select
            listing_id,
            month,
            available,
            row_number() over (partition by listing_id order by month) as _id
        from month_available
    ),
    second_table as (
        select
            listing_id,
            month,
            available,
            _id,
            row_number() over (partition by listing_id order by month) as _ranking
        from first_table
        where available = true
    ),
    third_table as (
        select listing_id, month, available, _id, _ranking, (_id - _ranking) as island
        from second_table
    ),
    fourth_table as (
        select
            listing_id,
            island,
            min(_id) as island_min,
            max(_id) as island_max,
            max(_id) - min(_id) as diff
        from third_table
        group by listing_id, island
    ),
    result as (
        select listing_id, max(diff) + 1 as max_consecutive_months
        from fourth_table
        group by listing_id
    )

select listing_id, max_consecutive_months
from result

union all

select distinct listing_id, 0 as max_consecutive_months
from {{ ref('fct_calendar_info') }}
where listing_id not in (select listing_id from result)
