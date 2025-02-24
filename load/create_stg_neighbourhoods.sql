create extension postgis;

create table if not exists airbnb.public.stg_neighbourhoods (
    neighbourhood text not null,
    neighbourhood_group text,
    geometry geometry (multipolygon, 4326)
);
