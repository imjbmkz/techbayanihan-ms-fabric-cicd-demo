# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a05dbecc-1c93-44b9-8c73-8b6bff7026b1",
# META       "default_lakehouse_name": "AwesomeLH_dev",
# META       "default_lakehouse_workspace_id": "e1127f86-76fc-45a2-8c21-350a41e41a8d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a05dbecc-1c93-44b9-8c73-8b6bff7026b1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_01_ingest_nyc_tlc
# Lands NYC TLC source files, unchanged, in the **data lake** layer (`Files/`) of the default lakehouse.
# | Source | URL | Lands in |
# |---|---|---|
# | Trip records (per month) | `{source_base_url}/trip-data/{trip_type}_tripdata_{YYYY}-{MM}.parquet` | `{datalake_root}/{trip_type}_taxi_trips/year=YYYY/month=MM/` |
# | Taxi zone lookup (fixed) | `{source_base_url}/misc/taxi_zone_lookup.csv` | `{datalake_root}/taxi_zones/taxi_zone_lookup.csv` |
# | Taxi zone shapefile (fixed) | `{source_base_url}/misc/taxi_zones.zip` | `{datalake_root}/taxi_zones/taxi_zones.zip` (+ extracted to `shapefile/`) |
# **Orchestration**
# - Every value in the parameters cell can be overridden by a pipeline *Notebook activity*. Pass `run_id = @pipeline().RunId` and `pipeline_name = @pipeline().Pipeline` so logs can be traced back to the pipeline run.
# - Idempotent: files already landed are skipped unless `overwrite = true`, so retries are safe.
# - Exits with a JSON summary (`notebookutils.notebook.exit`) that the pipeline can read: `@activity('<name>').output.result.exitValue`.
# - The run **fails** (activity turns red) if any file fails, or if a month is not published and `fail_on_missing = true`. Logs are written first.
# **Runtime logs and errors**
# | Where | Grain | Content |
# |---|---|---|
# | `ops.ingestion_runs` (Delta) | 1 row per run | parameters, status, counts, duration, error + traceback |
# | `ops.ingestion_log` (Delta) | 1 row per file | URL, target path, status, rows, bytes, sha256, duration, error + traceback |
# | `Files/logs/nb_01_ingest_nyc_tlc/date=YYYY-MM-DD/<run_id>.log` | text | full runtime log |
# Paths are relative to the *default lakehouse*, so the same code runs in dev / QA / prod after fabric-cicd rebinds it.


# PARAMETERS CELL ********************

# --- Scope --------------------------------------------------------------
start_month = "2026-01"          # first trip month, YYYY-MM
end_month = "2026-09"            # last trip month, YYYY-MM ("" = same as start_month)
trip_type = "yellow"             # yellow | green | fhv | fhvhv
include_trips = True             # land monthly trip parquet files
include_reference = True         # land taxi_zone_lookup.csv + taxi_zones.zip
extract_zones_zip = True         # also unzip the shapefile into taxi_zones/shapefile/

# --- Source / target ----------------------------------------------------
source_base_url = "https://d37ci6vzurychx.cloudfront.net"
datalake_root = "Files/datalake/nyc_tlc"

# --- Behaviour ----------------------------------------------------------
overwrite = False                # re-download files that already exist
fail_on_missing = False          # fail the run if a trip month isn't published yet

# --- Orchestration / logging ---------------------------------------------
run_id = ""                      # pipeline: @pipeline().RunId  ("" = generated)
pipeline_name = ""               # pipeline: @pipeline().Pipeline ("" = manual run)
log_schema = "ops"
log_folder = "Files/logs"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Setup: imports, run context, logger
# ---------------------------------------------------------------------------
import hashlib
import io
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import traceback
import uuid
import zipfile
from datetime import datetime, timezone

import requests

NOTEBOOK_NAME = "nb_01_ingest_nyc_tlc"
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
TRIP_TYPES = {"yellow", "green", "fhv", "fhvhv"}
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Fabric notebook; nyc-tlc-cicd-demo)"}
CHUNK_SIZE = 8 * 1024 * 1024
MAX_RETRIES = 3


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_bool(value) -> bool:
    """Pipelines often pass booleans as strings ("true"/"false")."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


run_id = str(run_id).strip() or str(uuid.uuid4())
pipeline_name = str(pipeline_name).strip() or "manual"
run_started_at = utc_now()

# Logger -> notebook output + in-memory buffer (persisted to Files/logs at the end)
log_buffer = io.StringIO()
logger = logging.getLogger(NOTEBOOK_NAME)
logger.setLevel(logging.INFO)
logger.propagate = False
for h in list(logger.handlers):  # avoid duplicate handlers when re-running cells
    logger.removeHandler(h)
_fmt = logging.Formatter(f"%(asctime)s | %(levelname)-7s | run={run_id[:8]} | %(message)s",
                         datefmt="%Y-%m-%dT%H:%M:%SZ")
_fmt.converter = time.gmtime
for handler in (logging.StreamHandler(sys.stdout), logging.StreamHandler(log_buffer)):
    handler.setFormatter(_fmt)
    logger.addHandler(handler)

# Shared state the remaining cells fill in. Cells never raise until the last one,
# so logs are always persisted even when something goes wrong.
run_error = None       # {"message": ..., "traceback": ...} for run-level failures
tasks = []             # what to ingest
file_results = []      # one dict per task

logger.info(f"Starting {NOTEBOOK_NAME} (pipeline={pipeline_name})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Helpers: month range, download, validation
# ---------------------------------------------------------------------------

def month_range(start: str, end: str) -> list:
    """Inclusive list of YYYY-MM strings between start and end."""
    start, end = str(start).strip(), (str(end).strip() or str(start).strip())
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


class NotPublished(Exception):
    """Source returned 403/404: file not (yet) published."""


def download_to_temp(url: str, suffix: str) -> str:
    """Stream URL to a local temp file with retries; returns the temp path."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, headers=HTTP_HEADERS, stream=True, timeout=(10, 300)) as r:
                if r.status_code in (403, 404):
                    raise NotPublished(f"HTTP {r.status_code} for {url}")
                r.raise_for_status()
                fd, tmp = tempfile.mkstemp(suffix=suffix)
                with os.fdopen(fd, "wb") as f:
                    for chunk in r.iter_content(CHUNK_SIZE):
                        f.write(chunk)
                return tmp
        except NotPublished:
            raise
        except Exception as e:  # network hiccups, 5xx, timeouts
            last_error = e
            logger.warning(f"  attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
    raise RuntimeError(f"Download failed after {MAX_RETRIES} attempts: {url}") from last_error


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(block)
    return h.hexdigest()


def validate_parquet(path: str) -> int:
    import pyarrow.parquet as pq
    return pq.ParquetFile(path).metadata.num_rows


def validate_csv(path: str) -> int:
    with open(path, "r", encoding="utf-8-sig") as f:
        header = f.readline()
        if "LocationID" not in header:
            raise ValueError(f"Unexpected CSV header: {header.strip()[:200]}")
        return sum(1 for line in f if line.strip())


def validate_zip(path: str) -> int:
    if not zipfile.is_zipfile(path):
        raise ValueError("Downloaded file is not a valid zip archive")
    with zipfile.ZipFile(path) as z:
        bad = z.testzip()
        if bad:
            raise ValueError(f"Corrupt member in zip: {bad}")
        return len(z.namelist())


VALIDATORS = {"parquet": validate_parquet, "csv": validate_csv, "zip": validate_zip}


def extract_zip_to_lake(local_zip: str, lake_folder: str) -> int:
    """Unzip locally, then copy each member into the lakehouse folder."""
    tmp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(local_zip) as z:
            z.extractall(tmp_dir)
        count = 0
        for root, _, files in os.walk(tmp_dir):
            for name in files:
                local_path = os.path.join(root, name)
                rel = os.path.relpath(local_path, tmp_dir).replace(os.sep, "/")
                dest = f"{lake_folder}/{rel}"
                notebookutils.fs.mkdirs(dest.rsplit("/", 1)[0])
                notebookutils.fs.cp(f"file:{local_path}", dest)
                count += 1
        return count
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Build the task list from parameters
# ---------------------------------------------------------------------------
try:
    trip_type = str(trip_type).strip().lower()
    include_trips = to_bool(include_trips)
    include_reference = to_bool(include_reference)
    extract_zones_zip = to_bool(extract_zones_zip)
    overwrite = to_bool(overwrite)
    fail_on_missing = to_bool(fail_on_missing)
    base = source_base_url.rstrip("/")
    root = datalake_root.rstrip("/")

    if trip_type not in TRIP_TYPES:
        raise ValueError(f"Invalid trip_type '{trip_type}'. Expected one of {sorted(TRIP_TYPES)}.")
    if not (include_trips or include_reference):
        raise ValueError("Nothing to do: include_trips and include_reference are both false.")

    months = month_range(start_month, end_month) if include_trips else []
    for m in months:
        yyyy, mm = m.split("-")
        file_name = f"{trip_type}_tripdata_{yyyy}-{mm}.parquet"
        tasks.append({
            "dataset": f"{trip_type}_taxi_trips",
            "period": m,
            "file_type": "parquet",
            "url": f"{base}/trip-data/{file_name}",
            "target_path": f"{root}/{trip_type}_taxi_trips/year={yyyy}/month={mm}/{file_name}",
            "required": fail_on_missing,
        })

    if include_reference:
        tasks.append({
            "dataset": "taxi_zone_lookup",
            "period": None,
            "file_type": "csv",
            "url": f"{base}/misc/taxi_zone_lookup.csv",
            "target_path": f"{root}/taxi_zones/taxi_zone_lookup.csv",
            "required": True,
        })
        tasks.append({
            "dataset": "taxi_zones_shapefile",
            "period": None,
            "file_type": "zip",
            "url": f"{base}/misc/taxi_zones.zip",
            "target_path": f"{root}/taxi_zones/taxi_zones.zip",
            "required": True,
            "extract_to": f"{root}/taxi_zones/shapefile" if extract_zones_zip else None,
        })

    logger.info(f"Trip type      : {trip_type if include_trips else '(trips disabled)'}")
    if months:
        logger.info(f"Months         : {months[0]} .. {months[-1]} ({len(months)})")
    logger.info(f"Reference data : {include_reference} (extract zip: {extract_zones_zip})")
    logger.info(f"Data lake root : {root}")
    logger.info(f"Overwrite      : {overwrite} | fail_on_missing: {fail_on_missing}")
    logger.info(f"Tasks          : {len(tasks)}")
except Exception as e:
    run_error = {"message": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc()}
    logger.error(f"Invalid parameters: {run_error['message']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Ingest: one task at a time; errors are captured per file, never raised here
# ---------------------------------------------------------------------------

def run_task(task: dict) -> dict:
    started = utc_now()
    result = {**task, "status": None, "rows": None, "bytes": None, "sha256": None,
              "extracted_files": None, "error_message": None, "error_traceback": None,
              "started_at": started}
    label = f"{task['dataset']}" + (f" {task['period']}" if task["period"] else "")
    logger.info(f"[{label}] {task['url']}")

    tmp = None
    try:
        if not overwrite and notebookutils.fs.exists(task["target_path"]):
            result["status"] = "skipped"
            logger.info(f"[{label}] skipped: already landed (overwrite=false)")
            return result

        tmp = download_to_temp(task["url"], suffix=f".{task['file_type']}")
        result["bytes"] = os.path.getsize(tmp)
        result["rows"] = VALIDATORS[task["file_type"]](tmp)  # rows (parquet/csv) or members (zip)
        result["sha256"] = sha256_of(tmp)

        notebookutils.fs.mkdirs(task["target_path"].rsplit("/", 1)[0])
        notebookutils.fs.cp(f"file:{tmp}", task["target_path"])

        if task.get("extract_to"):
            result["extracted_files"] = extract_zip_to_lake(tmp, task["extract_to"])
            logger.info(f"[{label}] extracted {result['extracted_files']} files -> {task['extract_to']}")

        result["status"] = "loaded"
        logger.info(f"[{label}] loaded: {result['rows']:,} {'members' if task['file_type'] == 'zip' else 'rows'}, "
                    f"{result['bytes'] / 1024 / 1024:.1f} MB -> {task['target_path']}")
    except NotPublished as e:
        result["status"] = "not_available"
        result["error_message"] = str(e)
        log = logger.error if task["required"] else logger.warning
        log(f"[{label}] not available: {e}")
    except Exception as e:
        result["status"] = "failed"
        result["error_message"] = f"{type(e).__name__}: {e}"
        result["error_traceback"] = traceback.format_exc()
        logger.error(f"[{label}] FAILED: {result['error_message']}")
    finally:
        if tmp and os.path.exists(tmp):
            os.remove(tmp)
        result["ended_at"] = utc_now()
        result["duration_s"] = round((result["ended_at"] - started).total_seconds(), 2)
    return result


if run_error is None:
    try:
        for task in tasks:
            file_results.append(run_task(task))
    except Exception as e:  # unexpected failure outside a single task
        run_error = {"message": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc()}
        logger.error(f"Run aborted: {run_error['message']}")
else:
    logger.warning("Skipping ingestion because of a run-level error.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Summarise + persist logs (Delta tables + text log file)
# ---------------------------------------------------------------------------
counts = {s: sum(r["status"] == s for r in file_results)
          for s in ("loaded", "skipped", "not_available", "failed")}
missing_required = [r for r in file_results if r["status"] == "not_available" and r["required"]]

if run_error:
    run_status = "failed"
elif counts["failed"] or missing_required:
    run_status = "failed"
elif counts["not_available"]:
    run_status = "succeeded_with_warnings"
else:
    run_status = "succeeded"

run_ended_at = utc_now()
log_file = f"{log_folder.rstrip('/')}/{NOTEBOOK_NAME}/date={run_started_at:%Y-%m-%d}/{run_id}.log"
run_parameters = {
    "start_month": start_month, "end_month": end_month, "trip_type": trip_type,
    "include_trips": include_trips, "include_reference": include_reference,
    "extract_zones_zip": extract_zones_zip, "source_base_url": source_base_url,
    "datalake_root": datalake_root, "overwrite": overwrite, "fail_on_missing": fail_on_missing,
}
run_error_message = run_error["message"] if run_error else (
    "; ".join(f"{' '.join(filter(None, [r['dataset'], r['period']]))}: {r['error_message']}"
              for r in file_results if r["status"] == "failed" or r in missing_required) or None)

summary = {
    "notebook": NOTEBOOK_NAME,
    "run_id": run_id,
    "pipeline_name": pipeline_name,
    "status": run_status,
    **counts,
    "duration_s": round((run_ended_at - run_started_at).total_seconds(), 2),
    "log_file": log_file,
    "error": run_error_message,
}
logger.info(f"Run {run_status}: " + ", ".join(f"{k}={v}" for k, v in counts.items()))


def persist_logs():
    from pyspark.sql.types import (IntegerType, LongType, DoubleType, StringType,
                                   StructField, StructType, TimestampType)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {log_schema}")

    runs_schema = StructType([
        StructField("run_id", StringType()), StructField("notebook", StringType()),
        StructField("pipeline_name", StringType()), StructField("status", StringType()),
        StructField("parameters", StringType()), StructField("tasks_total", IntegerType()),
        StructField("loaded", IntegerType()), StructField("skipped", IntegerType()),
        StructField("not_available", IntegerType()), StructField("failed", IntegerType()),
        StructField("started_at", TimestampType()), StructField("ended_at", TimestampType()),
        StructField("duration_s", DoubleType()), StructField("log_file", StringType()),
        StructField("error_message", StringType()), StructField("error_traceback", StringType()),
    ])
    runs_row = [(
        run_id, NOTEBOOK_NAME, pipeline_name, run_status, json.dumps(run_parameters),
        len(tasks), counts["loaded"], counts["skipped"], counts["not_available"], counts["failed"],
        run_started_at, run_ended_at, summary["duration_s"], log_file,
        run_error_message, run_error["traceback"] if run_error else None,
    )]
    (spark.createDataFrame(runs_row, runs_schema)
          .write.mode("append").option("mergeSchema", "true")
          .saveAsTable(f"{log_schema}.ingestion_runs"))

    if file_results:
        log_schema_def = StructType([
            StructField("run_id", StringType()), StructField("notebook", StringType()),
            StructField("dataset", StringType()), StructField("period", StringType()),
            StructField("file_type", StringType()), StructField("source_url", StringType()),
            StructField("target_path", StringType()), StructField("status", StringType()),
            StructField("rows", LongType()), StructField("bytes", LongType()),
            StructField("sha256", StringType()), StructField("extracted_files", IntegerType()),
            StructField("started_at", TimestampType()), StructField("ended_at", TimestampType()),
            StructField("duration_s", DoubleType()), StructField("error_message", StringType()),
            StructField("error_traceback", StringType()),
        ])
        rows = [(
            run_id, NOTEBOOK_NAME, r["dataset"], r["period"], r["file_type"], r["url"],
            r["target_path"], r["status"], r["rows"], r["bytes"], r["sha256"], r["extracted_files"],
            r["started_at"], r["ended_at"], float(r["duration_s"]), r["error_message"], r["error_traceback"],
        ) for r in file_results]
        (spark.createDataFrame(rows, log_schema_def)
              .write.mode("append").option("mergeSchema", "true")
              .saveAsTable(f"{log_schema}.ingestion_log"))


# Logging must never hide the real outcome: failures here are reported, not raised.
try:
    persist_logs()
    logger.info(f"Logs written to {log_schema}.ingestion_runs / {log_schema}.ingestion_log")
except Exception as e:
    logger.error(f"Could not write log tables: {type(e).__name__}: {e}")

try:
    notebookutils.fs.mkdirs(log_file.rsplit("/", 1)[0])
    notebookutils.fs.put(log_file, log_buffer.getvalue(), True)
    print(f"Log file: {log_file}")
except Exception as e:
    print(f"Could not write log file {log_file}: {type(e).__name__}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Hand the result to the orchestrator
# ---------------------------------------------------------------------------
print(json.dumps(summary, indent=2))

if run_status == "failed":
    # Makes the pipeline activity fail; details are already in the log tables/file.
    raise RuntimeError(f"{NOTEBOOK_NAME} failed: {json.dumps(summary)}")

notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
