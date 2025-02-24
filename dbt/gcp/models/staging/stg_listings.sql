select
    safe_cast(id as integer) as id,
    safe_cast(listing_url as string) as listing_url,
    safe_cast(scrape_id as integer) as scrape_id,
    safe_cast(last_scraped as date) as last_scraped,
    safe_cast(source as string) as source,
    safe_cast(name as string) as name,
    safe_cast(description as string) as description,
    safe_cast(neighborhood_overview as string) as neighborhood_overview,
    safe_cast(picture_url as string) as picture_url,
    safe_cast(host_id as integer) as host_id,
    safe_cast(host_url as string) as host_url,
    safe_cast(host_name as string) as host_name,
    safe_cast(host_since as date) as host_since,
    safe_cast(host_location as string) as host_location,
    safe_cast(host_about as string) as host_about,
    safe_cast(host_response_time as string) as host_response_time,
    safe_cast(trim(host_response_rate, '%') as float64) / 100 as host_response_rate,
    safe_cast(trim(host_acceptance_rate, '%') as float64) / 100 as host_acceptance_rate,
    (
        case host_is_superhost when 't' then true when 'f' then false end
    ) as host_is_superhost,
    safe_cast(host_thumbnail_url as string) as host_thumbnail_url,
    safe_cast(host_picture_url as string) as host_picture_url,
    safe_cast(host_neighbourhood as string) as host_neighbourhood,
    safe_cast(host_listings_count as integer) as host_listings_count,
    safe_cast(host_total_listings_count as integer) as host_total_listings_count,
    array(
        select json_extract_scalar(string_element)
        from
            unnest(
                json_extract_array(safe_cast(host_verifications as string))
            ) as string_element
    ) as host_verifications,
    (
        case host_has_profile_pic when 't' then true when 'f' then false end
    ) as host_has_profile_pic,
    (
        case host_identity_verified when 't' then true when 'f' then false end
    ) as host_identity_verified,
    safe_cast(neighbourhood as string) as neighbourhood,
    safe_cast(neighbourhood_cleansed as string) as neighbourhood_cleansed,
    safe_cast(neighbourhood_group_cleansed as string) as neighbourhood_group_cleansed,
    safe_cast(latitude as float64) as latitude,
    safe_cast(longitude as float64) as longitude,
    safe_cast(property_type as string) as property_type,
    safe_cast(room_type as string) as room_type,
    safe_cast(accommodates as integer) as accommodates,
    safe_cast(bathrooms as integer) as bathrooms,
    safe_cast(bathrooms_text as string) as bathrooms_text,
    safe_cast(bedrooms as integer) as bedrooms,
    safe_cast(beds as integer) as beds,
    array(
        select json_extract_scalar(string_element)
        from
            unnest(json_extract_array(safe_cast(amenities as string))) as string_element
    ) as amenities,
    safe_cast(regexp_replace(price, "[$,]", "") as float64) as price,
    safe_cast(minimum_nights as integer) as minimum_nights,
    safe_cast(maximum_nights as integer) as maximum_nights,
    safe_cast(minimum_minimum_nights as integer) as minimum_minimum_nights,
    safe_cast(maximum_minimum_nights as integer) as maximum_minimum_nights,
    safe_cast(minimum_maximum_nights as integer) as minimum_maximum_nights,
    safe_cast(maximum_maximum_nights as integer) as maximum_maximum_nights,
    safe_cast(minimum_nights_avg_ntm as float64) as minimum_nights_avg_ntm,
    safe_cast(maximum_nights_avg_ntm as float64) as maximum_nights_avg_ntm,
    safe_cast(safe_cast(calendar_updated as string) as date) as calendar_updated,
    (
        case has_availability when 't' then true when 'f' then false end
    ) as has_availability,
    safe_cast(availability_30 as integer) as availability_30,
    safe_cast(availability_60 as integer) as availability_60,
    safe_cast(availability_90 as integer) as availability_90,
    safe_cast(availability_365 as integer) as availability_365,
    safe_cast(calendar_last_scraped as date) as calendar_last_scraped,
    safe_cast(number_of_reviews as integer) as number_of_reviews,
    safe_cast(number_of_reviews_ltm as integer) as number_of_reviews_ltm,
    safe_cast(number_of_reviews_l30d as integer) as number_of_reviews_l30d,
    safe_cast(first_review as date) as first_review,
    safe_cast(last_review as date) as last_review,
    safe_cast(review_scores_rating as float64) as review_scores_rating,
    safe_cast(review_scores_accuracy as float64) as review_scores_accuracy,
    safe_cast(review_scores_cleanliness as float64) as review_scores_cleanliness,
    safe_cast(review_scores_checkin as float64) as review_scores_checkin,
    safe_cast(review_scores_communication as float64) as review_scores_communication,
    safe_cast(review_scores_location as float64) as review_scores_location,
    safe_cast(review_scores_value as float64) as review_scores_value,
    safe_cast(license as string) as license,
    (
        case instant_bookable when 't' then true when 'f' then false end
    ) as instant_bookable,
    safe_cast(
        calculated_host_listings_count as integer
    ) as calculated_host_listings_count,
    safe_cast(
        calculated_host_listings_count_entire_homes as integer
    ) as calculated_host_listings_count_entire_homes,
    safe_cast(
        calculated_host_listings_count_private_rooms as integer
    ) as calculated_host_listings_count_private_rooms,
    safe_cast(
        calculated_host_listings_count_shared_rooms as integer
    ) as calculated_host_listings_count_shared_rooms,
    safe_cast(reviews_per_month as float64) as reviews_per_month
from {{ source("dataset", "raw_listings") }}
