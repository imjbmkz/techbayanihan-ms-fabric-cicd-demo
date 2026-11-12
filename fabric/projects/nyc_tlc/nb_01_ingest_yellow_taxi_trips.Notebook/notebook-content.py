# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_01_ingest_yellow_taxi_trips
# Downloads NYC TLC **yellow taxi trip** parquet files from the public TLC endpoint into the **data lake** layer of the default lakehouse:
# ```
# Files/datalake/nyc_tlc/yellow_taxi_trips/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
# ```
# **Environment-agnostic:** paths are relative to the notebook's *default lakehouse*, so the same code runs in dev / QA / prod once fabric-cicd rebinds the default lakehouse (`AwesomeLH_dev` → `AwesomeLH_qa` / `AwesomeLH_prod`).
# **Parameters** (next cell is the *parameters cell*, so pipeline Notebook activities can override them):
# | Parameter | Default | Meaning |
# |---|---|---|
# | `start_month` | `2024-01` | First month to load (`YYYY-MM`) |
# | `end_month` | `""` | Last month to load (`YYYY-MM`); empty = same as `start_month` |
# | `source_base_url` | TLC CloudFront URL | Where the parquet files are published |
# | `file_prefix` | `yellow_tripdata` | File name prefix on the source |
# | `target_folder` | `Files/datalake/nyc_tlc/yellow_taxi_trips` | Landing folder in the default lakehouse |
# | `overwrite` | `false` | Re-download files that already exist |
# | `fail_on_missing` | `false` | Fail the run if a month isn't published yet |
# The notebook exits with a JSON summary (`notebookutils.notebook.exit`) that the pipeline can read.


# PARAMETERS CELL ********************

start_month = "2024-01"
end_month = ""
source_base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
file_prefix = "yellow_tripdata"
target_folder = "Files/datalake/nyc_tlc/yellow_taxi_trips"
overwrite = False
fail_on_missing = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone

import pyarrow.parquet as pq
import requests

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Fabric notebook; nyc-tlc-cicd-demo)"}
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
MAX_RETRIES = 3


def to_bool(value) -> bool:
    """Pipelines may pass booleans as strings ("true"/"false")."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def month_range(start: str, end: str) -> list:
    """Inclusive list of YYYY-MM strings between start and end."""
    start, end = start.strip(), (end or start).strip()
    for m in (start, end):
        if not MONTH_PATTERN.match(m):
            raise ValueError(f"Invalid month '{m}'. Expected YYYY-MM, e.g. 2024-01.")
    if end < start:
        raise ValueError(f"end_month ({end}) is before start_month ({start}).")

    year, month = map(int, start.split("-"))
    months = []
    while f"{year:04d}-{month:02d}" <= end:
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return months


def target_path(folder: str, prefix: str, month: str) -> str:
    year, mm = month.split("-")
    return f"{folder.rstrip('/')}/year={year}/month={mm}/{prefix}_{month}.parquet"


def download_to_temp(url: str) -> str:
    """Stream a URL to a local temp file with simple retries. Returns the temp path.
    Raises FileNotFoundError when the file isn't published (TLC returns 403/404)."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, headers=HTTP_HEADERS, stream=True, timeout=(10, 300)) as r:
                if r.status_code in (403, 404):
                    raise FileNotFoundError(f"Not published: {url} (HTTP {r.status_code})")
                r.raise_for_status()
                fd, tmp = tempfile.mkstemp(suffix=".parquet")
                with os.fdopen(fd, "wb") as f:
                    for chunk in r.iter_content(CHUNK_SIZE):
                        f.write(chunk)
                return tmp
        except FileNotFoundError:
            raise
        except Exception as e:  # network hiccups, 5xx, timeouts
            last_error = e
            print(f"  attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(5 * attempt)
    raise RuntimeError(f"Download failed after {MAX_RETRIES} attempts: {url}") from last_error

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

overwrite = to_bool(overwrite)
fail_on_missing = to_bool(fail_on_missing)
months = month_range(start_month, end_month)

print(f"Months to load : {months[0]} .. {months[-1]} ({len(months)})")
print(f"Source         : {source_base_url}/{file_prefix}_<YYYY-MM>.parquet")
print(f"Target         : {target_folder}")
print(f"Overwrite      : {overwrite}")

results = []
for month in months:
    file_name = f"{file_prefix}_{month}.parquet"
    url = f"{source_base_url.rstrip('/')}/{file_name}"
    dest = target_path(target_folder, file_prefix, month)
    result = {"month": month, "url": url, "path": dest}
    print(f"\n[{month}]")

    if notebookutils.fs.exists(dest) and not overwrite:
        print("  skipped: already in data lake (set overwrite=true to reload)")
        results.append({**result, "status": "skipped"})
        continue

    tmp = None
    try:
        tmp = download_to_temp(url)
        size_mb = os.path.getsize(tmp) / 1024 / 1024
        num_rows = pq.ParquetFile(tmp).metadata.num_rows  # validates it's a real parquet file

        notebookutils.fs.mkdirs(dest.rsplit("/", 1)[0])
        notebookutils.fs.cp(f"file:{tmp}", dest)

        print(f"  loaded: {num_rows:,} rows, {size_mb:.1f} MB -> {dest}")
        results.append({**result, "status": "loaded", "rows": num_rows, "size_mb": round(size_mb, 1)})
    except FileNotFoundError as e:
        print(f"  not available: {e}")
        results.append({**result, "status": "not_available"})
    except Exception as e:
        print(f"  FAILED: {e}")
        results.append({**result, "status": "failed", "error": str(e)})
    finally:
        if tmp and os.path.exists(tmp):
            os.remove(tmp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = {
    "notebook": "nb_01_ingest_datalake",
    "run_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "start_month": months[0],
    "end_month": months[-1],
    "target_folder": target_folder,
    "loaded": sum(r["status"] == "loaded" for r in results),
    "skipped": sum(r["status"] == "skipped" for r in results),
    "not_available": sum(r["status"] == "not_available" for r in results),
    "failed": sum(r["status"] == "failed" for r in results),
    "files": results,
}
print(json.dumps({k: v for k, v in summary.items() if k != "files"}, indent=2))

if summary["failed"] or (fail_on_missing and summary["not_available"]):
    raise RuntimeError(f"Ingestion finished with problems: {json.dumps(summary)}")

notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
