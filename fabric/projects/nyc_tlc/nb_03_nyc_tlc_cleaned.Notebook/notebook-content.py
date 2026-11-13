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

# # nb_03_nyc_tlc_cleaned (raw → cleaned)
# | Object | Type | Content |
# |---|---|---|
# | `cleaned.nyc_tlc__yellow_taxi_trips__dq` | Delta table | every raw trip, typed + renamed, with `dq_failed_rules` (array), `dq_is_valid` |
# | `cleaned.nyc_tlc__yellow_taxi_trips__cleaned` | materialized lake view | rows with `dq_is_valid = true` |
# | `cleaned.nyc_tlc__yellow_taxi_trips__quarantined` | materialized lake view | rows with `dq_is_valid = false` + the rules they failed |
# | `cleaned.nyc_tlc__taxi_zone_lookup__cleaned` | materialized lake view | typed, trimmed zone lookup (no DQ rules) |
# **DQ rules (yellow taxi trips)**
# | Rule | Fails when |
# |---|---|
# | `missing_timestamps` | pickup or dropoff is null |
# | `dropoff_not_after_pickup` | dropoff ≤ pickup |
# | `trip_longer_than_24h` | duration > 24 h |
# | `pickup_outside_file_month` | pickup isn't in the month of the source file (`_src_year`/`_src_month`) |
# | `invalid_trip_distance` | null, negative or > 500 miles |
# | `invalid_passenger_count` | < 0 or > 9 (null allowed) |
# | `negative_amount` | `fare_amount` or `total_amount` < 0 (refunds/voids) |
# | `invalid_location_id` | pickup/dropoff zone missing or outside 1–265 |
# Every object is rebuilt with `CREATE OR REPLACE` on each run, so the definitions in Git are always what's deployed. A rule summary is saved in the step `details` of `ops.notebook_steps`.

# PARAMETERS CELL ********************

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

log = RunLogger("nb_03_nyc_tlc_cleaned", run_id=run_id, pipeline_name=pipeline_name)

SRC_TRIPS = "raw.nyc_tlc__yellow_taxi_trips__raw"
SRC_ZONES = "raw.nyc_tlc__taxi_zone_lookup__raw"
DQ_TABLE = "cleaned.nyc_tlc__yellow_taxi_trips__dq"
CLEANED_MLV = "cleaned.nyc_tlc__yellow_taxi_trips__cleaned"
QUARANTINE_MLV = "cleaned.nyc_tlc__yellow_taxi_trips__quarantined"
ZONES_MLV = "cleaned.nyc_tlc__taxi_zone_lookup__cleaned"

# Business columns shared by the dq table and both views (Spark SQL has no SELECT * EXCEPT)
TRIP_COLUMNS = """
    vendor_id, pickup_datetime, dropoff_datetime, trip_duration_min, passenger_count,
    trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
    payment_type_id, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee,
    total_amount, source_year, source_month, _source_file, _ingested_at"""

log.step("create schema", "CREATE SCHEMA IF NOT EXISTS cleaned")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("build trips dq table", f"""
CREATE OR REPLACE TABLE {DQ_TABLE}
USING DELTA
COMMENT 'Yellow taxi trips from raw, typed, with data quality flags'
AS
WITH typed AS (
    SELECT
        CAST(vendorid AS INT)                           AS vendor_id,
        CAST(tpep_pickup_datetime AS TIMESTAMP)         AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)        AS dropoff_datetime,
        CAST(passenger_count AS INT)                    AS passenger_count,
        CAST(trip_distance AS DOUBLE)                   AS trip_distance,
        CAST(ratecodeid AS INT)                         AS rate_code_id,
        CAST(store_and_fwd_flag AS STRING)              AS store_and_fwd_flag,
        CAST(pulocationid AS INT)                       AS pu_location_id,
        CAST(dolocationid AS INT)                       AS do_location_id,
        CAST(payment_type AS INT)                       AS payment_type_id,
        CAST(fare_amount AS DECIMAL(10, 2))             AS fare_amount,
        CAST(extra AS DECIMAL(10, 2))                   AS extra,
        CAST(mta_tax AS DECIMAL(10, 2))                 AS mta_tax,
        CAST(tip_amount AS DECIMAL(10, 2))              AS tip_amount,
        CAST(tolls_amount AS DECIMAL(10, 2))            AS tolls_amount,
        CAST(improvement_surcharge AS DECIMAL(10, 2))   AS improvement_surcharge,
        CAST(congestion_surcharge AS DECIMAL(10, 2))    AS congestion_surcharge,
        CAST(airport_fee AS DECIMAL(10, 2))             AS airport_fee,
        CAST(cbd_congestion_fee AS DECIMAL(10, 2))      AS cbd_congestion_fee,
        CAST(total_amount AS DECIMAL(10, 2))            AS total_amount,
        CAST(_src_year AS INT)                          AS source_year,
        CAST(_src_month AS INT)                         AS source_month,
        _source_file,
        _ingested_at
    FROM {SRC_TRIPS}
),
checked AS (
    SELECT
        *,
        ROUND((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60.0, 2)
            AS trip_duration_min,
        filter(array(
            CASE WHEN pickup_datetime IS NULL OR dropoff_datetime IS NULL
                 THEN 'missing_timestamps' END,
            CASE WHEN dropoff_datetime <= pickup_datetime
                 THEN 'dropoff_not_after_pickup' END,
            CASE WHEN unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime) > 86400
                 THEN 'trip_longer_than_24h' END,
            CASE WHEN year(pickup_datetime) <> source_year OR month(pickup_datetime) <> source_month
                 THEN 'pickup_outside_file_month' END,
            CASE WHEN trip_distance IS NULL OR trip_distance < 0 OR trip_distance > 500
                 THEN 'invalid_trip_distance' END,
            CASE WHEN passenger_count < 0 OR passenger_count > 9
                 THEN 'invalid_passenger_count' END,
            CASE WHEN fare_amount < 0 OR total_amount < 0
                 THEN 'negative_amount' END,
            CASE WHEN pu_location_id IS NULL OR do_location_id IS NULL
                   OR pu_location_id NOT BETWEEN 1 AND 265
                   OR do_location_id NOT BETWEEN 1 AND 265
                 THEN 'invalid_location_id' END
        ), rule -> rule IS NOT NULL) AS dq_failed_rules
    FROM typed
)
SELECT
    {TRIP_COLUMNS},
    dq_failed_rules,
    size(dq_failed_rules) = 0 AS dq_is_valid,
    current_timestamp()       AS dq_checked_at
FROM checked
""", target=DQ_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dq_summary() -> dict:
    totals = spark.sql(f"""
        SELECT count(*)                                   AS total,
               count_if(dq_is_valid)                      AS passed,
               count_if(NOT dq_is_valid)                  AS quarantined
        FROM {DQ_TABLE}
    """).first()
    rules = spark.sql(f"""
        SELECT rule, count(*) AS failures
        FROM {DQ_TABLE} LATERAL VIEW explode(dq_failed_rules) r AS rule
        GROUP BY rule ORDER BY failures DESC
    """).collect()
    pct = round(100.0 * totals["passed"] / totals["total"], 2) if totals["total"] else None
    result = {"rows": totals["total"], "passed": totals["passed"], "quarantined": totals["quarantined"],
              "passed_pct": pct, "rule_failures": {r["rule"]: r["failures"] for r in rules}}
    log.info(f"DQ: {json.dumps(result)}")
    return result


log.step("dq summary", dq_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("mlv: trips cleaned (passed)", f"""
CREATE OR REPLACE MATERIALIZED LAKE VIEW {CLEANED_MLV}
COMMENT "Yellow taxi trips that passed all data quality rules"
AS
SELECT {TRIP_COLUMNS}
FROM {DQ_TABLE}
WHERE dq_is_valid
""", target=CLEANED_MLV)

log.step("mlv: trips quarantined (failed)", f"""
CREATE OR REPLACE MATERIALIZED LAKE VIEW {QUARANTINE_MLV}
COMMENT "Yellow taxi trips that failed at least one data quality rule"
AS
SELECT {TRIP_COLUMNS}, dq_failed_rules, dq_checked_at
FROM {DQ_TABLE}
WHERE NOT dq_is_valid
""", target=QUARANTINE_MLV)

log.step("mlv: taxi zone lookup", f"""
CREATE OR REPLACE MATERIALIZED LAKE VIEW {ZONES_MLV}
COMMENT "Taxi zone lookup, typed and trimmed"
AS
SELECT
    CAST(locationid AS INT)     AS location_id,
    trim(borough)               AS borough,
    trim(zone)                  AS zone,
    trim(service_zone)          AS service_zone,
    _source_file,
    _ingested_at
FROM {SRC_ZONES}
""", target=ZONES_MLV)

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
