import argparse
import json
import logging
import os
import signal
from pathlib import Path

import pandas as pd
from flask import Flask

PID = os.getpid()
app = Flask(__name__)


class Progress:
    def __init__(self, src, chunksize):
        # api parameters
        self.src = src
        self.chunksize = chunksize

        # default values
        self.reader = None
        self.index = 0  # current source
        self.count = 0  # records read from the current source
        self.num_records = None  # total number of records from the current source

    def get(self):
        return self.reader, self.index, self.src, self.chunksize, self.count, self.num_records

    def set(self, reader, index, count, num_records):
        self.reader = reader
        self.index = index
        self.count = count
        self.num_records = num_records


@app.route("/<source>", methods=["GET"])
def extract_data(source):
    """
    The GET method to simulate obtaining data from a web scraping api.
    Call http://127.0.0.1:5000/<source> to fetch data.
    """

    assert source in ["neighbourhoods", "listings", "reviews", "calendar"]
    global progress
    reader, index, src, chunksize, count, num_records = progress[source].get()

    # neighbourhoods
    if source == "neighbourhoods":
        if index < len(src):
            geojson_file = Path(src[index]) / "neighbourhoods.geojson"
            with open(geojson_file, encoding="utf-8") as f:
                geojson = json.load(f)
            app.logger.info(f"neighbourhoods - from source ({index+1}/{len(src)}): {src[index]}")
            progress[source].set(None, index + 1, None, None)
        else:
            geojson = {}
            app.logger.info("Web scraping ended. No more data")
        return geojson

    # listings, reviews, calendar
    gz_file = f"{source}.csv.gz"
    msg = "{0} - record ({1}/{2}) from source ({3}/{4}): {5}"

    if reader is None:
        current_src = Path(src[index]) / gz_file
        reader = pd.read_csv(current_src, chunksize=chunksize)
        num_records = len(pd.read_csv(current_src))

    try:
        chunk = next(reader)
        count += len(chunk)
        app.logger.info(msg.format(source, count, num_records, index + 1, len(src), src[index]))
        progress[source].set(reader, index, count, num_records)
        return chunk.to_dict("records")

    except StopIteration:
        if index < len(src) - 1:
            count, index = 0, index + 1
            current_src = Path(src[index]) / gz_file
            reader = pd.read_csv(current_src, chunksize=chunksize)
            num_records = len(pd.read_csv(current_src))
            progress[source].set(reader, index, count, num_records)
            return extract_data(source)
        else:
            progress[source].set(None, None, None, None)
            app.logger.info("Web scraping ended. No more data")
            return []


@app.route("/shutdown", methods=["GET"])
def shutdown():
    """
    The GET method to close connection.
    Call http://127.0.0.1:5000/shutdown to shut down the server.
    """
    pid = os.getpid()
    assert pid == PID
    os.kill(pid, signal.SIGINT)
    return "OK", 200


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to demonstrate argparse usage.")
    parser.add_argument("src_dir", type=str, help="The directory of data sources.")
    parser.add_argument(
        "--chunk_size", type=int, default=1, help="chunk size used when reading data sources."
    )

    args = parser.parse_args()
    path = Path(args.src_dir)
    # dates = sorted([str(subdir.resolve()) for subdir in path.iterdir() if subdir.is_dir()])
    dates = [path]
    chunksize = args.chunk_size

    print(f"PID = {PID}")
    print(f"src_dir = {args.src_dir}")
    print(f"dates = {dates}")
    print(f"chunk_size = {args.chunk_size}")

    global progress
    progress = {}
    progress["listings"] = Progress(dates, chunksize)
    progress["reviews"] = Progress(dates, chunksize)
    progress["calendar"] = Progress(dates, chunksize)
    progress["neighbourhoods"] = Progress(dates, None)

    app.logger.setLevel(logging.INFO)
    # Note: app.run will return normally after SIGINT.
    # You will not see the exceptions below. Also not if you hit
    # CTRL+C.
    # See: https://stackoverflow.com/a/78399447
    try:
        app.run(debug=False)
        print("App run ended")
    except KeyboardInterrupt as exc:
        print(f"Caught KeyboardInterrupt {exc}")
    except BaseException as exc:
        print(f"Caught exception {exc.__class__.__name__}: {exc}")
