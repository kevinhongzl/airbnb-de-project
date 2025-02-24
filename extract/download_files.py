import warnings
from pathlib import Path

from requests import get


def download_files_if_not_exist(source, ds):
    files = [
        ("visualisations", "listings.csv"),
        ("visualisations", "reviews.csv"),
        ("visualisations", "neighbourhoods.csv"),
        ("visualisations", "neighbourhoods.geojson"),
        ("data", "listings.csv.gz"),
        ("data", "calendar.csv.gz"),
        ("data", "reviews.csv.gz"),
    ]
    task_state = "success"

    for ftype, f in files:
        fpath = Path(f"{source}/{ds}/{f}")
        if Path(fpath).exists():
            print(f"{ds}/{f} ... exists.")
            continue
        try:
            url = f"https://data.insideairbnb.com/taiwan/northern-taiwan/taipei/{ds}/{ftype}/{f}"
            fpath.parent.mkdir(exist_ok=True, parents=True)
            fp = open(fpath, "wb")
            content = get(url, stream=True).content

            if "AccessDenied" in content.decode("ascii"):
                fp.close()
                fpath.unlink()  # remove the 0-byte file due to unsuccess download
                task_state = "fail"
                print(f"{ds}/{f} ... can NOT be downloaded.")
            else:
                msg = "valid binary files may not be able to be decoded. This is fine."
                raise UnicodeDecodeError("success", b"\x00\x00", 1, 2, msg)

        except UnicodeDecodeError:
            # valid binary files may not be able to be decoded.
            fp.write(content)
            fp.close()
            print(f"{ds}/{f} ... successfully downloaded.")
            print(url)
        except Exception as e:
            print(f"{ds}/{f} ... can NOT be downloaded.")
            print(e)

    # remove the empty directory
    if next(fpath.parent.iterdir(), None) is None:
        fpath.parent.rmdir()
        warnings.warn(f"\n{str(fpath.parent)} is removed since it contains no file.")

    # Mark the task "sucess" in airflow but leave a warning in log
    if task_state == "fail":
        warnings.warn("\nOne of the downloads is failed and could impact downstream tasks.")
