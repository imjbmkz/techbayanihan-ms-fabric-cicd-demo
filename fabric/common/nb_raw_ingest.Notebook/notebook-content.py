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

# # nb_raw_ingest (metadata-driven)
# Loads files from the **data lake** (`Files/...`) into **raw** Delta tables. *What* to load comes from the control table `ops.raw_ingestion_config`, not from code, so a new dataset needs only config rows (see `nb_02_nyc_tlc_raw_config`).
# | Config column | Meaning |
# |---|---|
# | `source_id` | unique key, e.g. `nyc_tlc__yellow_taxi_trips` |
# | `domain`, `dataset` | used for filtering and naming |
# | `source_path`, `file_pattern` | folder under the lakehouse (searched recursively) + file name glob |
# | `file_format`, `read_options` | Spark reader format + JSON options (e.g. `{"header": "true"}`) |
# | `load_mode` | `incremental` = only new/changed files are appended · `full` = table rebuilt from all files |
# | `target_table` | e.g. `raw.nyc_tlc__yellow_taxi_trips__raw` |
# | `normalize_column_names` | lower-case + `_` (avoids `Airport_fee` vs `airport_fee` drift) |
# | `load_order`, `is_active` | processing order / on-off switch |
# Raw tables keep the source columns *as delivered* plus lineage columns: `_source_file`, `_source_modified_at`, `_ingested_at`, `_run_id`, and `_src_<key>` for every `key=value` folder in the path (e.g. `_src_year`, `_src_month`).
# **Incremental logic:** a file is loaded if its path isn't in the target yet, or if it was re-landed with a newer modified time (its old rows are deleted first). Column types are aligned to the existing table; new columns are added (`mergeSchema`).


# PARAMETERS CELL ********************

# --- Scope ----------------------------------------------------------------
domain = "nyc_tlc"               # which domain's config rows to process
source_ids = ""                  # optional comma-separated source_id filter ("" = all active)
force_full_reload = False        # rebuild every selected table from all files
config_table = "ops.raw_ingestion_config"

# --- Orchestration / logging ---------------------------------------------
run_id = ""                      # pipeline: @pipeline().RunId  ("" = generated)
pipeline_name = ""               # pipeline: @pipeline().Pipeline ("" = manual run)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_common_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fnmatch
from pyspark.sql import functions as F

NOTEBOOK_NAME = "nb_raw_ingest"
log = RunLogger(NOTEBOOK_NAME, run_id=run_id, pipeline_name=pipeline_name, parameters={
    "domain": domain, "source_ids": source_ids, "force_full_reload": force_full_reload,
    "config_table": config_table,
})

IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
TABLE_NAME = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


def normalize_name(name: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def relative_path(path: str) -> str:
    """abfss://.../<lakehouse>/Files/a/b.parquet -> Files/a/b.parquet (environment-agnostic)."""
    idx = path.find("/Files/")
    return path[idx + 1:] if idx >= 0 else path


def list_files(root: str, pattern: str) -> list:
    """Recursive listing under `root` (relative to the default lakehouse) filtered by a glob."""
    found, stack = [], [root.rstrip("/")]
    while stack:
        for item in notebookutils.fs.ls(stack.pop()):
            if item.isDir:
                stack.append(item.path)
            elif fnmatch.fnmatch(item.name, pattern):
                found.append({"path": relative_path(item.path), "name": item.name,
                              "size": item.size,
                              "modified_at": datetime.fromtimestamp(item.modifyTime / 1000, tz=timezone.utc)})
    return sorted(found, key=lambda f: f["path"])


def path_partitions(path: str) -> dict:
    """Files/x/year=2026/month=01/f.parquet -> {"_src_year": "2026", "_src_month": "01"}"""
    return {f"_src_{normalize_name(k)}": v for k, v in
            (seg.split("=", 1) for seg in path.split("/")[:-1] if "=" in seg)}


def read_source_file(cfg: dict, file: dict):
    reader = spark.read.format(cfg["file_format"])
    if cfg["read_options"]:
        reader = reader.options(**json.loads(cfg["read_options"]))
    df = reader.load(file["path"])
    if cfg["normalize_column_names"]:
        df = df.toDF(*[normalize_name(c) for c in df.columns])
    for col_name, value in path_partitions(file["path"]).items():
        df = df.withColumn(col_name, F.lit(value))
    return (df.withColumn("_source_file", F.lit(file["path"]))
              .withColumn("_source_modified_at", F.lit(file["modified_at"]).cast("timestamp"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_run_id", F.lit(log.run_id)))


def align_types(df, target: str):
    """Cast columns to the existing target types so monthly files with drifting types still append."""
    if not table_exists(target):
        return df
    target_types = {f.name: f.dataType for f in spark.table(target).schema.fields}
    for field in df.schema.fields:
        wanted = target_types.get(field.name)
        if wanted is not None and wanted != field.dataType:
            df = df.withColumn(field.name, F.col(field.name).cast(wanted))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Read the configuration
# ---------------------------------------------------------------------------
configs = []


def load_config():
    if not IDENTIFIER.match(str(domain)):
        raise ValueError(f"Invalid domain '{domain}'")
    if not TABLE_NAME.match(config_table):
        raise ValueError(f"Invalid config_table '{config_table}'")
    rows = spark.sql(f"""
        SELECT *
        FROM {config_table}
        WHERE is_active AND domain = {sql_literal(domain)}
        ORDER BY load_order, source_id
    """).collect()
    selected = {s.strip() for s in str(source_ids).split(",") if s.strip()}
    for r in rows:
        if selected and r["source_id"] not in selected:
            continue
        if not TABLE_NAME.match(r["target_table"]):
            raise ValueError(f"Invalid target_table '{r['target_table']}' for {r['source_id']}")
        if r["load_mode"] not in ("incremental", "full"):
            raise ValueError(f"Invalid load_mode '{r['load_mode']}' for {r['source_id']}")
        configs.append(r.asDict())
    missing = selected - {c["source_id"] for c in configs}
    if missing:
        raise ValueError(f"source_ids not found/active for domain '{domain}': {sorted(missing)}")
    if not configs:
        raise ValueError(f"No active sources in {config_table} for domain '{domain}'")
    return {"rows": len(configs), "sources": [c["source_id"] for c in configs]}


log.step(f"read config ({config_table})", load_config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Load each source: plan (which files) -> one step per file
# ---------------------------------------------------------------------------
force_full_reload = to_bool(force_full_reload)


def plan_source(cfg: dict) -> dict:
    files = list_files(cfg["source_path"], cfg["file_pattern"])
    if not files:
        raise FileNotFoundError(f"No files matching '{cfg['file_pattern']}' under {cfg['source_path']}")
    target = cfg["target_table"]
    full = force_full_reload or cfg["load_mode"] == "full" or not table_exists(target)
    if full:
        to_load = [{**f, "action": "full"} for f in files]
    else:
        loaded = {r["_source_file"]: r["modified_at"] for r in spark.sql(
            f"SELECT _source_file, max(_source_modified_at) AS modified_at FROM {target} GROUP BY _source_file"
        ).collect()}
        to_load = []
        for f in files:
            prev = loaded.get(f["path"])
            if prev is None:
                to_load.append({**f, "action": "new"})
            elif f["modified_at"].replace(tzinfo=None) > prev.replace(tzinfo=None):
                to_load.append({**f, "action": "changed"})
    cfg["_plan"] = {"full": full, "files": to_load}
    return {"rows": len(to_load), "files_found": len(files), "files_to_load": len(to_load),
            "mode": "full" if full else "incremental"}


def load_file(cfg: dict, file: dict, first_in_full_load: bool) -> dict:
    target = cfg["target_table"]
    df = read_source_file(cfg, file)
    if not first_in_full_load:
        df = align_types(df, target)
    rows = df.count()

    writer = df.write.format("delta").option("mergeSchema", "true")
    if first_in_full_load:
        writer = writer.mode("overwrite").option("overwriteSchema", "true")
    else:
        writer = writer.mode("append")
        if file["action"] == "changed":
            spark.sql(f"DELETE FROM {target} WHERE _source_file = {sql_literal(file['path'])}")
    writer.saveAsTable(target)
    return {"rows": rows, "source_file": file["path"], "action": file["action"],
            "size_mb": round(file["size"] / 1024 / 1024, 2)}


for cfg in configs:
    sid = cfg["source_id"]
    plan = log.step(f"{sid}: plan", lambda cfg=cfg: plan_source(cfg))
    if plan is None:
        continue
    if not cfg["_plan"]["files"]:
        log.info(f"{sid}: nothing new to load")
        continue
    if cfg["_plan"]["full"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['target_table'].split('.')[0]}")
    for i, f in enumerate(cfg["_plan"]["files"]):
        log.step(f"{sid}: load {f['name']} ({f['action']})",
                 lambda cfg=cfg, f=f, i=i: load_file(cfg, f, cfg["_plan"]["full"] and i == 0),
                 target=cfg["target_table"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.finish()
log.exit()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
