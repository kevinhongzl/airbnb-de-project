import ast
import re
from re import IGNORECASE, search

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
import sqlparse
from geoalchemy2 import Geometry, WKTElement
from google.cloud import bigquery
from sqlalchemy.dialects.postgresql import ARRAY, ENUM


def percentage2float(x):
    return float(x.replace("%", "")) / 100 if not pd.isna(x) else x


def price2float(x):
    return float(x.replace("$", "").replace(",", "")) if not pd.isna(x) else x


def read_file(file):
    ext = file.split(".")[-1]
    if ext in ["csv", "gz"]:
        data = pd.read_csv(file)
    elif ext == "feather":
        data = pd.read_feather(file)
    else:
        raise ValueError(f"Invailid file: {file}")
    return data


def print_data_info(df):
    print(f"The data has {df.shape[0]} rows and {df.shape[1]} columns.\n")
    df.info()
    print("\n")


def ingest_listings_data(file, engine):
    """Ingest listings.csv.gz to postgres database"""
    # parse data based on its file extension
    listings_data = read_file(file)

    # some basic transformations
    listings_data.host_since = pd.to_datetime(listings_data.host_since)
    listings_data.last_scraped = pd.to_datetime(listings_data.last_scraped)
    listings_data.calendar_updated = pd.to_datetime(listings_data.calendar_updated)
    listings_data.first_review = pd.to_datetime(listings_data.first_review)
    listings_data.last_review = pd.to_datetime(listings_data.last_review)
    listings_data.price = listings_data.price.apply(price2float)
    listings_data.host_response_rate = listings_data.host_response_rate.apply(percentage2float)
    listings_data.host_acceptance_rate = listings_data.host_acceptance_rate.apply(percentage2float)
    listings_data.host_verifications = listings_data.host_verifications.apply(ast.literal_eval)

    # load data into staging table in the database / data warehouse
    print(f"Loading {file} into database ... ")
    print_data_info(listings_data)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE airbnb.public.stg_listings"))
    # set keyword argument if_exist='append' and truncate staging table in advance
    # if if_exist='replace', it erases your table and builds a new one
    # the schema and the column data types we have maintained will no longer exist
    rows_affected = listings_data.to_sql("stg_listings", engine, index=False, if_exists="append")
    print(f"{rows_affected} rows created")


def ingest_reviews_data(file, engine):
    """Ingest reviews.csv.gz to postgres database"""

    reviews_data = read_file(file)
    print(f"Loading {file} into database ... ")
    print_data_info(reviews_data)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE airbnb.public.stg_reviews"))
    rows_affected = reviews_data.to_sql("stg_reviews", engine, index=False, if_exists="append")
    print(f"{rows_affected} rows created")


def ingest_neighbourhoods_data(file, engine):
    """Ingest neighbourhoods.geojson to postgres database"""

    # parse data based on its file extension
    neighbourhoods_data = gpd.read_file(file)
    # some basic transformations
    neighbourhoods_data["geometry"] = neighbourhoods_data["geometry"].apply(
        lambda geom: WKTElement(geom.wkt, srid=4326)
    )
    # remove old data and load new data into staging table
    print(f"Loading {file} into database ... ")
    print_data_info(neighbourhoods_data)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE airbnb.public.stg_neighbourhoods"))
    rows_affected = neighbourhoods_data.to_sql(
        "stg_neighbourhoods",
        engine,
        index=False,
        if_exists="append",
        dtype={"geometry": Geometry("MultiPloygon")},
    )
    print(f"{rows_affected} rows created")


def ingest_calendar_data(file, engine):
    """Ingest calendar.csc.gz to postgres database"""

    # parse data based on its file extension
    calendar_data = read_file(file)
    # some basic transformations
    calendar_data.available[calendar_data.available == "t"] = True
    calendar_data.available[calendar_data.available == "f"] = False
    calendar_data.price = calendar_data.price.apply(price2float)
    calendar_data.adjusted_price = calendar_data.adjusted_price.apply(price2float)
    # load data into staging table in the database / data warehouse
    print(f"Loading {file} into database ... ")
    print_data_info(calendar_data)
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE airbnb.public.stg_calendar"))
    rows_affected = calendar_data.to_sql("stg_calendar", engine, index=False, if_exists="append")
    print(f"{rows_affected} rows created")


def get_bq_schema_from_sql(file):
    with open(file, "r") as f:
        content = f.read()
    queries = sqlparse.split(sqlparse.format(content, strip_comments=True))
    for query in queries:
        pg_schema = parse_create_table(query, if_not_exists=True)
        if not pg_schema:
            pg_schema = parse_create_table(query)
        if pg_schema:
            break  # return the first CREATE clause
    return postgres_to_bq_schema(pg_schema)


def parse_create_table(query, if_not_exists=False):
    if if_not_exists:
        pattern = r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([\w\.]+)"
    else:
        pattern = r"CREATE\s+TABLE\s+([\w\.]+)"

    query_cleaned = re.sub(r"\n", r"", query)
    table_match = re.search(pattern, query_cleaned, re.IGNORECASE)
    column_match = re.search(r"\((.*)\)", re.sub(r"\n", r"", query_cleaned))

    if table_match and column_match:
        table_name = table_match.group(1).split(".")[-1]
        column_defs = column_match.group(1)

        column_pattern = r"\s+([\w_]+)\s+([^,]+)[\,]?"
        schema = re.findall(column_pattern, column_defs)
        return {"table_name": table_name, "schema": dict(schema)}
    else:
        return {}


def postgres_to_bq_schema(pg_schema):
    bq_schema = {"table_name": pg_schema["table_name"], "schema": []}
    for col, typ in pg_schema["schema"].items():
        # bigquery data types
        bq_typ = "STRING" if search("text", typ, IGNORECASE) else None
        bq_typ = "INT64" if search("int", typ, IGNORECASE) and bq_typ is None else bq_typ
        bq_typ = "BOOL" if search("bool", typ, IGNORECASE) and bq_typ is None else bq_typ
        bq_typ = "FLOAT64" if search("real", typ, IGNORECASE) and bq_typ is None else bq_typ
        bq_typ = "DATE" if search("date", typ, IGNORECASE) and bq_typ is None else bq_typ
        bq_typ = "GEOGRAPHY" if search("geometry", typ, IGNORECASE) and bq_typ is None else bq_typ
        if bq_typ is None:
            raise ValueError(f"Invalid date type {typ} of column {col}")

        # modes
        mode = "REQUIRED" if search(r"\bnot null\b|\bprimary key\b", typ, IGNORECASE) else None
        mode = "REPEATED" if search(r"\[\]", typ, re.IGNORECASE) and mode is None else mode
        mode = "NULLABLE" if mode is None else mode

        bq_schema["schema"].append(bigquery.SchemaField(col, bq_typ, mode=mode))

    return bq_schema
