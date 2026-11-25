# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a05dbecc-1c93-44b9-8c73-8b6bff7026b1",
# META       "default_lakehouse_name": "AwesomeLH",
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

# # nb_02_nyc_tlc_raw (data lake → raw, PySpark)
# | Raw table | Source (from `nb_01_ingest_nyc_tlc`) | Load |
# |---|---|---|
# | `raw.nyc_tlc__yellow_taxi_trips__raw` | `Files/datalake/nyc_tlc/yellow_taxi_trips/year=YYYY/month=MM/*.parquet` | **incremental**: only files not loaded yet |
# | `raw.nyc_tlc__taxi_zone_lookup__raw` | `Files/datalake/nyc_tlc/taxi_zones/taxi_zone_lookup.csv` | full refresh (265 rows) |
# **Incremental by file:** the raw table records the file each row came from (`_source_file`), and each run appends only files that aren't in the table yet. Re-running is safe. A file that was re-downloaded with `OVERWRITE` in `nb_01` isn't picked up again; use `FULL_RELOAD = True` for that.
# **Conformed schema:** monthly TLC files don't always use the same column types or casing (`Airport_fee` vs `airport_fee`). Each file is mapped to one fixed column list and types, so appends never fail on drift.
# *Alternative:* Spark Structured Streaming (`readStream` + checkpoint + `trigger(availableNow=True)`) also processes only new files. It needs one fixed schema for every file, which the drifting monthly files don't guarantee, so this notebook tracks files itself.


# PARAMETERS CELL ********************

DATALAKE_ROOT = "Files/datalake/nyc_tlc"   # where nb_01_ingest_nyc_tlc lands files
FULL_RELOAD = False                         # True = drop and rebuild the trips raw table from all files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import re
from pyspark.sql import functions as F

TRIPS_FOLDER = f"{DATALAKE_ROOT}/yellow_taxi_trips"
ZONES_FILE = f"{DATALAKE_ROOT}/taxi_zones/taxi_zone_lookup.csv"
TRIPS_TABLE = "raw.nyc_tlc__yellow_taxi_trips__raw"
ZONES_TABLE = "raw.nyc_tlc__taxi_zone_lookup__raw"

# One fixed column list + types for every monthly file (names lower-cased)
TRIP_COLUMNS = [
    ("vendorid", "int"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "bigint"),
    ("trip_distance", "double"),
    ("ratecodeid", "bigint"),
    ("store_and_fwd_flag", "string"),
    ("pulocationid", "int"),
    ("dolocationid", "int"),
    ("payment_type", "bigint"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
    ("cbd_congestion_fee", "double"),
]


def to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def table_exists(name: str) -> bool:
    try:
        spark.table(name).limit(0)
        return True
    except Exception:
        return False


def relative_path(path: str) -> str:
    """abfss://.../Files/a/b.parquet -> Files/a/b.parquet (same in every environment)."""
    i = path.find("/Files/")
    return path[i + 1:] if i >= 0 else path


def list_files(folder: str, suffix: str) -> list:
    found, stack = [], [folder]
    while stack:
        for item in notebookutils.fs.ls(stack.pop()):
            if item.isDir:
                stack.append(item.path)
            elif item.name.endswith(suffix):
                found.append(relative_path(item.path))
    return sorted(found)


def conform_trips(df, source_file: str):
    """Map one monthly file to TRIP_COLUMNS and add lineage columns."""
    by_lower = {c.lower(): c for c in df.columns}
    m = re.search(r"year=(\d{4})/month=(\d{2})", source_file)
    return df.select(
        *[(F.col(by_lower[name]) if name in by_lower else F.lit(None)).cast(dtype).alias(name)
          for name, dtype in TRIP_COLUMNS],
        F.lit(int(m.group(1)) if m else None).cast("int").alias("_src_year"),
        F.lit(int(m.group(2)) if m else None).cast("int").alias("_src_month"),
        F.lit(source_file).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


spark.sql("CREATE SCHEMA IF NOT EXISTS raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Yellow taxi trips (incremental)

# CELL ********************

if to_bool(FULL_RELOAD):
    print(f"FULL_RELOAD: dropping {TRIPS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {TRIPS_TABLE}")

files = list_files(TRIPS_FOLDER, ".parquet")
loaded = set()
if table_exists(TRIPS_TABLE):
    loaded = {r["_source_file"] for r in spark.sql(f"SELECT DISTINCT _source_file FROM {TRIPS_TABLE}").collect()}
new_files = [f for f in files if f not in loaded]

print(f"Files in data lake : {len(files)}")
print(f"Already in raw     : {len(loaded)}")
print(f"New to load        : {len(new_files)}")

for i, path in enumerate(new_files, 1):
    (conform_trips(spark.read.parquet(path), path)
        .write.format("delta")
        .mode("append")
        .partitionBy("_src_year", "_src_month")
        .saveAsTable(TRIPS_TABLE))
    print(f"  [{i}/{len(new_files)}] appended {path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Taxi zone lookup (full refresh)

# CELL ********************

zones = spark.read.option("header", "true").csv(ZONES_FILE)
zones = (zones.toDF(*[c.strip().lower() for c in zones.columns])
              .withColumn("_source_file", F.lit(ZONES_FILE))
              .withColumn("_ingested_at", F.current_timestamp()))
(zones.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(ZONES_TABLE))
print(f"{ZONES_TABLE}: {zones.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = {
    "notebook": "nb_02_nyc_tlc_raw",
    "trip_files_found": len(files),
    "trip_files_loaded": len(new_files),
    "trips_raw_rows": spark.table(TRIPS_TABLE).count() if table_exists(TRIPS_TABLE) else 0,
    "zones_raw_rows": spark.table(ZONES_TABLE).count(),
}
print(json.dumps(summary, indent=2))
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
