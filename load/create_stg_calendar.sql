create table if not exists airbnb.public.stg_calendar (
    listing_id varchar(255),
    date date,
    available boolean,
    price real,
    adjusted_price real,
    minimum_nights integer,
    maximum_night integer
);
