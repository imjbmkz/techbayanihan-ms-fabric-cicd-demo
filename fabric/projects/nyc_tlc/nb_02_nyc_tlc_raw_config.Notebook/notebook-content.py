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

# # nb_02_nyc_tlc_raw_config
# Declares the **nyc_tlc** sources for the metadata-driven raw ingestion (`nb_raw_ingest`).
# The rows below are the single source of truth: the MERGE inserts new sources, updates changed ones, and **deactivates** nyc_tlc rows that were removed here. Config lives in Git with the code and is deployed with it.
# After saving the config it **runs `nb_raw_ingest` for this domain** (set `run_ingestion = False` to only update the config). That child run writes its own logs to `ops.notebook_runs` / `ops.notebook_steps`.
# To onboard another dataset: copy this notebook (e.g. `nb_02_<domain>_raw_config`), change the rows, and run `nb_raw_ingest` with `domain = "<domain>"`.

# PARAMETERS CELL ********************

config_table = "ops.raw_ingestion_config"
run_ingestion = True             # also run nb_raw_ingest for this domain after updating the config
force_full_reload = False        # passed to nb_raw_ingest
ingestion_timeout_s = 3600       # max runtime for the nb_raw_ingest child run

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

DOMAIN = "nyc_tlc"
log = RunLogger("nb_02_nyc_tlc_raw_config", run_id=run_id, pipeline_name=pipeline_name,
                parameters={"config_table": config_table, "run_ingestion": run_ingestion,
                            "force_full_reload": force_full_reload})

log.step("create config table", f"""
CREATE TABLE IF NOT EXISTS {config_table} (
    source_id               STRING  COMMENT 'Unique key: <domain>__<dataset>',
    domain                  STRING,
    dataset                 STRING,
    source_path             STRING  COMMENT 'Folder in the default lakehouse, searched recursively',
    file_pattern            STRING  COMMENT 'File name glob, e.g. *.parquet',
    file_format             STRING  COMMENT 'Spark reader format: parquet | csv | json | ...',
    read_options            STRING  COMMENT 'JSON object of Spark reader options',
    load_mode               STRING  COMMENT 'incremental | full',
    target_table            STRING  COMMENT 'raw.<domain>__<dataset>__raw',
    normalize_column_names  BOOLEAN,
    load_order              INT,
    is_active               BOOLEAN,
    description             STRING,
    updated_at              TIMESTAMP
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("merge nyc_tlc sources", f"""
MERGE INTO {config_table} AS t
USING (
    SELECT * FROM VALUES
    (
        'nyc_tlc__yellow_taxi_trips', 'nyc_tlc', 'yellow_taxi_trips',
        'Files/datalake/nyc_tlc/yellow_taxi_trips', '*.parquet', 'parquet', '{{}}',
        'incremental', 'raw.nyc_tlc__yellow_taxi_trips__raw', true, 10, true,
        'NYC TLC yellow taxi trip records, one parquet file per month'
    ),
    (
        'nyc_tlc__taxi_zone_lookup', 'nyc_tlc', 'taxi_zone_lookup',
        'Files/datalake/nyc_tlc/taxi_zones', 'taxi_zone_lookup.csv', 'csv', '{{"header": "true"}}',
        'full', 'raw.nyc_tlc__taxi_zone_lookup__raw', true, 20, true,
        'NYC TLC taxi zone lookup (LocationID -> Borough, Zone, service_zone)'
    )
    AS s(source_id, domain, dataset, source_path, file_pattern, file_format, read_options,
         load_mode, target_table, normalize_column_names, load_order, is_active, description)
) AS s
ON t.source_id = s.source_id
WHEN MATCHED THEN UPDATE SET
    domain = s.domain, dataset = s.dataset, source_path = s.source_path,
    file_pattern = s.file_pattern, file_format = s.file_format, read_options = s.read_options,
    load_mode = s.load_mode, target_table = s.target_table,
    normalize_column_names = s.normalize_column_names, load_order = s.load_order,
    is_active = s.is_active, description = s.description, updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    source_id, domain, dataset, source_path, file_pattern, file_format, read_options, load_mode,
    target_table, normalize_column_names, load_order, is_active, description, updated_at
) VALUES (
    s.source_id, s.domain, s.dataset, s.source_path, s.file_pattern, s.file_format, s.read_options,
    s.load_mode, s.target_table, s.normalize_column_names, s.load_order, s.is_active,
    s.description, current_timestamp()
)
WHEN NOT MATCHED BY SOURCE AND t.domain = '{DOMAIN}' AND t.is_active THEN UPDATE SET
    is_active = false, updated_at = current_timestamp()
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("show config", lambda: display(spark.sql(
    f"SELECT * FROM {config_table} WHERE domain = '{DOMAIN}' ORDER BY load_order")))


def run_raw_ingest() -> dict:
    """Run the generic ingestion notebook for this domain (creates/loads the raw tables)."""
    exit_value = notebookutils.notebook.run("nb_raw_ingest", int(ingestion_timeout_s), {
        "domain": DOMAIN,
        "force_full_reload": to_bool(force_full_reload),
        "run_id": log.run_id,          # same run_id -> parent and child logs line up
        "pipeline_name": log.pipeline_name,
    })
    child = json.loads(exit_value) if exit_value else {}
    if child.get("status") != "succeeded":
        raise RuntimeError(f"nb_raw_ingest did not succeed: {exit_value}")
    return {"child_notebook": "nb_raw_ingest", "child_status": child.get("status"),
            "child_steps": child.get("steps"), "child_log_file": child.get("log_file")}


if to_bool(run_ingestion):
    log.step("run nb_raw_ingest (domain=nyc_tlc)", run_raw_ingest)
else:
    log.info("run_ingestion = False: config updated only; run nb_raw_ingest separately.")

for table in ("raw.nyc_tlc__yellow_taxi_trips__raw", "raw.nyc_tlc__taxi_zone_lookup__raw"):
    if not log.failed and to_bool(run_ingestion):
        log.step(f"check {table}", lambda table=table: spark.table(table).count(), target=table)

log.finish()
log.exit()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
