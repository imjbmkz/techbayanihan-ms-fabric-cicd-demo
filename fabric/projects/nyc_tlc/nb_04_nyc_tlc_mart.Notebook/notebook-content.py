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

# # nb_04_nyc_tlc_mart (cleaned → mart, Spark SQL)
# A simple star schema built from the **passed** trips view.
# | Table | Key |
# |---|---|
# | `mart.nyc_tlc__dim_date` | `date_key` (yyyyMMdd), full years covered by trips |
# | `mart.nyc_tlc__dim_zone` | `location_id` |
# | `mart.nyc_tlc__dim_vendor` | `vendor_id` |
# | `mart.nyc_tlc__dim_payment_type` | `payment_type_id` |
# | `mart.nyc_tlc__dim_rate_code` | `rate_code_id` (99 = unknown) |
# | `mart.nyc_tlc__fact_yellow_taxi_trips` | one row per trip; natural codes as keys |

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mart

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE mart.nyc_tlc__dim_date
# MAGIC USING DELTA
# MAGIC COMMENT 'Calendar for the full years covered by yellow taxi trips'
# MAGIC AS
# MAGIC WITH bounds AS (
# MAGIC     SELECT make_date(year(min(pickup_datetime)), 1, 1)   AS first_day,
# MAGIC            make_date(year(max(pickup_datetime)), 12, 31) AS last_day
# MAGIC     FROM cleaned.nyc_tlc__yellow_taxi_trips__cleaned
# MAGIC ),
# MAGIC days AS (
# MAGIC     SELECT explode(sequence(first_day, last_day, INTERVAL 1 DAY)) AS date FROM bounds
# MAGIC )
# MAGIC SELECT
# MAGIC     CAST(date_format(date, 'yyyyMMdd') AS INT)  AS date_key,
# MAGIC     date,
# MAGIC     year(date)                                  AS year,
# MAGIC     quarter(date)                               AS quarter,
# MAGIC     month(date)                                 AS month,
# MAGIC     date_format(date, 'MMMM')                   AS month_name,
# MAGIC     date_format(date, 'yyyy-MM')                AS year_month,
# MAGIC     day(date)                                   AS day_of_month,
# MAGIC     dayofweek(date)                             AS day_of_week,
# MAGIC     date_format(date, 'EEEE')                   AS day_name,
# MAGIC     dayofweek(date) IN (1, 7)                   AS is_weekend
# MAGIC FROM days

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE mart.nyc_tlc__dim_zone
# MAGIC USING DELTA
# MAGIC COMMENT 'Taxi zones (TLC LocationID)'
# MAGIC AS
# MAGIC SELECT location_id, borough, zone, service_zone
# MAGIC FROM cleaned.nyc_tlc__taxi_zone_lookup__cleaned

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE mart.nyc_tlc__dim_vendor
# MAGIC USING DELTA
# MAGIC COMMENT 'TPEP provider (TLC data dictionary)'
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (1, 'Creative Mobile Technologies, LLC'),
# MAGIC     (2, 'Curb Mobility, LLC'),
# MAGIC     (6, 'Myle Technologies Inc'),
# MAGIC     (7, 'Helix')
# MAGIC AS v(vendor_id, vendor_name)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE mart.nyc_tlc__dim_payment_type
# MAGIC USING DELTA
# MAGIC COMMENT 'Payment type (TLC data dictionary)'
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (0, 'Flex Fare trip'),
# MAGIC     (1, 'Credit card'),
# MAGIC     (2, 'Cash'),
# MAGIC     (3, 'No charge'),
# MAGIC     (4, 'Dispute'),
# MAGIC     (5, 'Unknown'),
# MAGIC     (6, 'Voided trip')
# MAGIC AS v(payment_type_id, payment_type_name)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE mart.nyc_tlc__dim_rate_code
# MAGIC USING DELTA
# MAGIC COMMENT 'Rate code (TLC data dictionary); 99 = null/unknown'
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (1, 'Standard rate'),
# MAGIC     (2, 'JFK'),
# MAGIC     (3, 'Newark'),
# MAGIC     (4, 'Nassau or Westchester'),
# MAGIC     (5, 'Negotiated fare'),
# MAGIC     (6, 'Group ride'),
# MAGIC     (99, 'Null/unknown')
# MAGIC AS v(rate_code_id, rate_code_name)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE mart.nyc_tlc__fact_yellow_taxi_trips
# MAGIC USING DELTA
# MAGIC COMMENT 'Yellow taxi trips that passed DQ, one row per trip'
# MAGIC AS
# MAGIC SELECT
# MAGIC     CAST(date_format(pickup_datetime, 'yyyyMMdd') AS INT)   AS pickup_date_key,
# MAGIC     hour(pickup_datetime)                                   AS pickup_hour,
# MAGIC     CAST(date_format(dropoff_datetime, 'yyyyMMdd') AS INT)  AS dropoff_date_key,
# MAGIC     vendor_id,
# MAGIC     payment_type_id,
# MAGIC     COALESCE(rate_code_id, 99)                              AS rate_code_id,
# MAGIC     pu_location_id,
# MAGIC     do_location_id,
# MAGIC     pickup_datetime,
# MAGIC     dropoff_datetime,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     trip_duration_min,
# MAGIC     fare_amount,
# MAGIC     extra,
# MAGIC     mta_tax,
# MAGIC     tip_amount,
# MAGIC     tolls_amount,
# MAGIC     improvement_surcharge,
# MAGIC     congestion_surcharge,
# MAGIC     airport_fee,
# MAGIC     cbd_congestion_fee,
# MAGIC     total_amount
# MAGIC FROM cleaned.nyc_tlc__yellow_taxi_trips__cleaned

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'dim_date' AS table_name, count(*) AS row_count FROM mart.nyc_tlc__dim_date
# MAGIC UNION ALL SELECT 'dim_zone', count(*) FROM mart.nyc_tlc__dim_zone
# MAGIC UNION ALL SELECT 'fact_yellow_taxi_trips', count(*) FROM mart.nyc_tlc__fact_yellow_taxi_trips

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
