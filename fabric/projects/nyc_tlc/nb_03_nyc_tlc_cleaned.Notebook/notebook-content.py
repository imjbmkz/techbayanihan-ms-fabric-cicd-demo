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

# # nb_03_nyc_tlc_cleaned (raw → cleaned, Spark SQL)
# | Object | Type | Content |
# |---|---|---|
# | `cleaned.nyc_tlc__yellow_taxi_trips__dq` | table | every raw trip, typed + renamed, with `dq_failed_rules` and `dq_is_valid` |
# | `cleaned.nyc_tlc__yellow_taxi_trips__cleaned` | materialized lake view | trips that passed every rule |
# | `cleaned.nyc_tlc__yellow_taxi_trips__quarantined` | materialized lake view | trips that failed at least one rule |
# | `cleaned.nyc_tlc__taxi_zone_lookup__cleaned` | materialized lake view | typed, trimmed zone lookup |
# **DQ rules:** `missing_timestamps`, `dropoff_not_after_pickup`, `trip_longer_than_24h`, `pickup_outside_file_month`, `invalid_trip_distance` (null, < 0 or > 500 mi), `invalid_passenger_count` (< 0 or > 9), `negative_amount`, `invalid_location_id` (outside 1–265).

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cleaned

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE cleaned.nyc_tlc__yellow_taxi_trips__dq
# MAGIC USING DELTA
# MAGIC COMMENT 'Yellow taxi trips from raw, typed, with data quality flags'
# MAGIC AS
# MAGIC WITH typed AS (
# MAGIC     SELECT
# MAGIC         vendorid                                    AS vendor_id,
# MAGIC         tpep_pickup_datetime                        AS pickup_datetime,
# MAGIC         tpep_dropoff_datetime                       AS dropoff_datetime,
# MAGIC         CAST(passenger_count AS INT)                AS passenger_count,
# MAGIC         trip_distance,
# MAGIC         CAST(ratecodeid AS INT)                     AS rate_code_id,
# MAGIC         store_and_fwd_flag,
# MAGIC         pulocationid                                AS pu_location_id,
# MAGIC         dolocationid                                AS do_location_id,
# MAGIC         CAST(payment_type AS INT)                   AS payment_type_id,
# MAGIC         CAST(fare_amount AS DECIMAL(10, 2))         AS fare_amount,
# MAGIC         CAST(extra AS DECIMAL(10, 2))               AS extra,
# MAGIC         CAST(mta_tax AS DECIMAL(10, 2))             AS mta_tax,
# MAGIC         CAST(tip_amount AS DECIMAL(10, 2))          AS tip_amount,
# MAGIC         CAST(tolls_amount AS DECIMAL(10, 2))        AS tolls_amount,
# MAGIC         CAST(improvement_surcharge AS DECIMAL(10, 2)) AS improvement_surcharge,
# MAGIC         CAST(congestion_surcharge AS DECIMAL(10, 2))  AS congestion_surcharge,
# MAGIC         CAST(airport_fee AS DECIMAL(10, 2))         AS airport_fee,
# MAGIC         CAST(cbd_congestion_fee AS DECIMAL(10, 2))  AS cbd_congestion_fee,
# MAGIC         CAST(total_amount AS DECIMAL(10, 2))        AS total_amount,
# MAGIC         _src_year                                   AS source_year,
# MAGIC         _src_month                                  AS source_month,
# MAGIC         _source_file,
# MAGIC         _ingested_at
# MAGIC     FROM raw.nyc_tlc__yellow_taxi_trips__raw
# MAGIC ),
# MAGIC checked AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROUND((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60.0, 2) AS trip_duration_min,
# MAGIC         filter(array(
# MAGIC             CASE WHEN pickup_datetime IS NULL OR dropoff_datetime IS NULL THEN 'missing_timestamps' END,
# MAGIC             CASE WHEN dropoff_datetime <= pickup_datetime THEN 'dropoff_not_after_pickup' END,
# MAGIC             CASE WHEN unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime) > 86400 THEN 'trip_longer_than_24h' END,
# MAGIC             CASE WHEN year(pickup_datetime) <> source_year OR month(pickup_datetime) <> source_month THEN 'pickup_outside_file_month' END,
# MAGIC             CASE WHEN trip_distance IS NULL OR trip_distance < 0 OR trip_distance > 500 THEN 'invalid_trip_distance' END,
# MAGIC             CASE WHEN passenger_count < 0 OR passenger_count > 9 THEN 'invalid_passenger_count' END,
# MAGIC             CASE WHEN fare_amount < 0 OR total_amount < 0 THEN 'negative_amount' END,
# MAGIC             CASE WHEN pu_location_id IS NULL OR do_location_id IS NULL
# MAGIC                    OR pu_location_id NOT BETWEEN 1 AND 265
# MAGIC                    OR do_location_id NOT BETWEEN 1 AND 265 THEN 'invalid_location_id' END
# MAGIC         ), rule -> rule IS NOT NULL) AS dq_failed_rules
# MAGIC     FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC     vendor_id, pickup_datetime, dropoff_datetime, trip_duration_min, passenger_count,
# MAGIC     trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
# MAGIC     payment_type_id, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
# MAGIC     improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee,
# MAGIC     total_amount, source_year, source_month, _source_file, _ingested_at,
# MAGIC     dq_failed_rules,
# MAGIC     size(dq_failed_rules) = 0 AS dq_is_valid,
# MAGIC     current_timestamp()       AS dq_checked_at
# MAGIC FROM checked

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DQ summary

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     count(*)                                              AS total_trips,
# MAGIC     count_if(dq_is_valid)                                 AS passed,
# MAGIC     count_if(NOT dq_is_valid)                             AS quarantined,
# MAGIC     round(100.0 * count_if(dq_is_valid) / count(*), 2)    AS passed_pct
# MAGIC FROM cleaned.nyc_tlc__yellow_taxi_trips__dq

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT rule, count(*) AS failures
# MAGIC FROM cleaned.nyc_tlc__yellow_taxi_trips__dq
# MAGIC LATERAL VIEW explode(dq_failed_rules) r AS rule
# MAGIC GROUP BY rule
# MAGIC ORDER BY failures DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Materialized lake views

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW cleaned.nyc_tlc__yellow_taxi_trips__cleaned
# MAGIC COMMENT "Yellow taxi trips that passed all data quality rules"
# MAGIC AS
# MAGIC SELECT
# MAGIC     vendor_id, pickup_datetime, dropoff_datetime, trip_duration_min, passenger_count,
# MAGIC     trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
# MAGIC     payment_type_id, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
# MAGIC     improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee,
# MAGIC     total_amount, source_year, source_month, _source_file, _ingested_at
# MAGIC FROM cleaned.nyc_tlc__yellow_taxi_trips__dq
# MAGIC WHERE dq_is_valid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW cleaned.nyc_tlc__yellow_taxi_trips__quarantined
# MAGIC COMMENT "Yellow taxi trips that failed at least one data quality rule"
# MAGIC AS
# MAGIC SELECT
# MAGIC     vendor_id, pickup_datetime, dropoff_datetime, trip_duration_min, passenger_count,
# MAGIC     trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id, do_location_id,
# MAGIC     payment_type_id, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
# MAGIC     improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee,
# MAGIC     total_amount, source_year, source_month, _source_file, _ingested_at,
# MAGIC     dq_failed_rules,
# MAGIC     dq_checked_at
# MAGIC FROM cleaned.nyc_tlc__yellow_taxi_trips__dq
# MAGIC WHERE NOT dq_is_valid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE MATERIALIZED LAKE VIEW cleaned.nyc_tlc__taxi_zone_lookup__cleaned
# MAGIC COMMENT "Taxi zone lookup, typed and trimmed"
# MAGIC AS
# MAGIC SELECT
# MAGIC     CAST(locationid AS INT)  AS location_id,
# MAGIC     trim(borough)            AS borough,
# MAGIC     trim(zone)               AS zone,
# MAGIC     trim(service_zone)       AS service_zone,
# MAGIC     _source_file,
# MAGIC     _ingested_at
# MAGIC FROM raw.nyc_tlc__taxi_zone_lookup__raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
