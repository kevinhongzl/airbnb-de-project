create table if not exists airbnb.public.stg_reviews (
    listing_id bigint not null,
    id bigint primary key,
    date date,
    reviewer_id bigint not null,
    reviewer_name text,
    comments text
);
