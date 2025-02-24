import argparse
import json
import time

import pandas as pd
import requests

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to demonstrate argparse usage.")
    parser.add_argument("src", type=str)
    parser.add_argument("dst", type=str)
    args = parser.parse_args()

    print("src =", args.src)
    print("dst =", args.dst)

    source = args.src
    url = "http://127.0.0.1:5000/{}"

    # Fetch Data from API
    time.sleep(7)  # wait for the api connection
    r = requests.get(url.format(source))
    records = r.json()
    while len(r.json()) > 0:
        r = requests.get(url.format(source))
        if source == "neighbourhoods":
            records.update(r.json())  # dict
        else:
            records += r.json()  # list

    print("Finish extracting data from API.")

    # Save data into a feather file
    try:
        if source == "neighbourhoods":
            with open(args.dst, "w") as f:
                json.dump(records, f)
        elif len(records) > 0:
            df = pd.DataFrame.from_records(records)
            # show info
            pd.set_option("display.max_rows", None)
            df.info(verbose=True)
            print(df.head(1).T)
            # save data
            df.to_feather(f"{args.dst}")
            print(f"Save data to {args.dst}")
    except Exception as e:
        raise e
    finally:
        # close connection
        try:
            requests.get(url.format("shutdown"))
        except Exception as e:
            print("Connection closed.")
