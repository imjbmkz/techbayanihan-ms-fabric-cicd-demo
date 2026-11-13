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

# # nb_04_nyc_tlc_mart (cleaned → mart)
# A simple star schema built from the **passed** trips view `cleaned.nyc_tlc__yellow_taxi_trips__cleaned`.
# | Table | Grain / key | Source |
# |---|---|---|
# | `mart.nyc_tlc__dim_date` | 1 row per day, `date_key` (yyyyMMdd) | generated for the full years covered by the trips |
# | `mart.nyc_tlc__dim_zone` | `location_id` | `cleaned.nyc_tlc__taxi_zone_lookup__cleaned` |
# | `mart.nyc_tlc__dim_vendor` | `vendor_id` | TLC data dictionary (static) |
# | `mart.nyc_tlc__dim_payment_type` | `payment_type_id` | TLC data dictionary (static) |
# | `mart.nyc_tlc__dim_rate_code` | `rate_code_id` | TLC data dictionary (static, 99 = unknown) |
# | `mart.nyc_tlc__fact_yellow_taxi_trips` | 1 row per trip | cleaned trips view |
# The fact uses the natural codes as keys (no surrogate keys), which keeps the model simple and Direct Lake friendly. Tables are rebuilt with `CREATE OR REPLACE TABLE ... AS SELECT` on each run.

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

log = RunLogger("nb_04_nyc_tlc_mart", run_id=run_id, pipeline_name=pipeline_name)

TRIPS = "cleaned.nyc_tlc__yellow_taxi_trips__cleaned"
ZONES = "cleaned.nyc_tlc__taxi_zone_lookup__cleaned"

log.step("create schema", "CREATE SCHEMA IF NOT EXISTS mart")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("dim_date", f"""
CREATE OR REPLACE TABLE mart.nyc_tlc__dim_date
USING DELTA
COMMENT 'Calendar for the full years covered by yellow taxi trips'
AS
WITH bounds AS (
    SELECT make_date(year(min(pickup_datetime)), 1, 1)   AS first_day,
           make_date(year(max(pickup_datetime)), 12, 31) AS last_day
    FROM {TRIPS}
),
days AS (
    SELECT explode(sequence(first_day, last_day, INTERVAL 1 DAY)) AS date FROM bounds
)
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INT)    AS date_key,
    date,
    year(date)                                    AS year,
    quarter(date)                                 AS quarter,
    month(date)                                   AS month,
    date_format(date, 'MMMM')                     AS month_name,
    date_format(date, 'yyyy-MM')                  AS year_month,
    day(date)                                     AS day_of_month,
    dayofweek(date)                               AS day_of_week,
    date_format(date, 'EEEE')                     AS day_name,
    dayofweek(date) IN (1, 7)                     AS is_weekend
FROM days
""", target="mart.nyc_tlc__dim_date")

log.step("dim_zone", f"""
CREATE OR REPLACE TABLE mart.nyc_tlc__dim_zone
USING DELTA
COMMENT 'Taxi zones (TLC LocationID)'
AS
SELECT location_id, borough, zone, service_zone
FROM {ZONES}
""", target="mart.nyc_tlc__dim_zone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("dim_vendor", """
CREATE OR REPLACE TABLE mart.nyc_tlc__dim_vendor
USING DELTA
COMMENT 'TPEP provider (TLC data dictionary)'
AS
SELECT * FROM VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'Curb Mobility, LLC'),
    (6, 'Myle Technologies Inc'),
    (7, 'Helix')
AS v(vendor_id, vendor_name)
""", target="mart.nyc_tlc__dim_vendor")

log.step("dim_payment_type", """
CREATE OR REPLACE TABLE mart.nyc_tlc__dim_payment_type
USING DELTA
COMMENT 'Payment type (TLC data dictionary)'
AS
SELECT * FROM VALUES
    (0, 'Flex Fare trip'),
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
AS v(payment_type_id, payment_type_name)
""", target="mart.nyc_tlc__dim_payment_type")

log.step("dim_rate_code", """
CREATE OR REPLACE TABLE mart.nyc_tlc__dim_rate_code
USING DELTA
COMMENT 'Rate code (TLC data dictionary); 99 = null/unknown'
AS
SELECT * FROM VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Null/unknown')
AS v(rate_code_id, rate_code_name)
""", target="mart.nyc_tlc__dim_rate_code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log.step("fact_yellow_taxi_trips", f"""
CREATE OR REPLACE TABLE mart.nyc_tlc__fact_yellow_taxi_trips
USING DELTA
COMMENT 'Yellow taxi trips that passed DQ, one row per trip'
AS
SELECT
    CAST(date_format(pickup_datetime, 'yyyyMMdd') AS INT)   AS pickup_date_key,
    hour(pickup_datetime)                                   AS pickup_hour,
    CAST(date_format(dropoff_datetime, 'yyyyMMdd') AS INT)  AS dropoff_date_key,
    vendor_id,
    payment_type_id,
    COALESCE(rate_code_id, 99)                              AS rate_code_id,
    pu_location_id,
    do_location_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    trip_duration_min,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount
FROM {TRIPS}
""", target="mart.nyc_tlc__fact_yellow_taxi_trips")

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
