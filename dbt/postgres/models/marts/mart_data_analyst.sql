with
    last_scraped_fct_listing_info as (
        select *
        from {{ ref('fct_listing_info') }}
        where
            scrape_id in (
                -- scrape_id of the last scraped 
                select scrape_id
                from {{ ref('dim_scrape') }}
                where
                    last_scraped
                    in (select max(last_scraped) from {{ ref('dim_scrape') }})
            )
    ),
    last_scraped_fct_host_info as (
        select *
        from {{ ref('fct_host_info') }}
        where
            scrape_id in (
                -- scrape_id of the last scraped 
                select scrape_id
                from {{ ref('dim_scrape') }}
                where
                    last_scraped
                    in (select max(last_scraped) from {{ ref('dim_scrape') }})
            )
    ),
    latest_dim_hosts as (
        select d_h.host_id as host_id, name as host_name, calculated_listings
        from {{ ref('dim_hosts') }} as d_h
        left join last_scraped_fct_host_info as f_h on d_h.host_id = f_h.host_id
    )

select
    f_lst.listing_id as id,
    d_lst.name as name,
    d_lst.host_id as host_id,
    host_name,
    neighbourhood_group,
    d_nbh.name as neighbourhood,
    latitude,
    longitude,
    room_type,
    price,
    minimum_nights,
    number_of_reviews,
    last_review,
    calculated_listings as calculated_host_listings_count,
    availability_365,
    number_of_reviews_ltm,
    license
from last_scraped_fct_listing_info as f_lst
inner join {{ ref('dim_listings') }} as d_lst on f_lst.listing_id = d_lst.listing_id
left join latest_dim_hosts as d_h on d_lst.host_id = d_h.host_id
left join
    {{ ref('dim_neighbourhoods') }} as d_nbh
    on d_lst.neighbourhood_id = d_nbh.neighbourhood_id
