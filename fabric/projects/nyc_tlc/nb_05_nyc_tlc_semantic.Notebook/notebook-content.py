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

# # nb_05_nyc_tlc_semantic (mart → semantic)
# `semantic.nyc_tlc__sem_daily_zone_trips`: one row per **day × pickup zone**, already joined to the date and zone attributes, so reports can use it directly or through a Direct Lake semantic model.
# | Measure | Definition |
# |---|---|
# | `trips`, `passengers` | count of trips, sum of passengers |
# | `total_distance_mi`, `avg_distance_mi` | trip distance |
# | `avg_duration_min` | average trip duration |
# | `total_fare`, `total_tips`, `total_revenue`, `avg_total_amount` | money (USD) |
# | `tip_pct_of_fare` | tips ÷ fare on card trips (cash tips aren't recorded) |
# | `card_trips`, `cash_trips` | trips by payment type |

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

log = RunLogger("nb_05_nyc_tlc_semantic", run_id=run_id, pipeline_name=pipeline_name)

log.step("create schema", "CREATE SCHEMA IF NOT EXISTS semantic")

log.step("sem_daily_zone_trips", """
CREATE OR REPLACE TABLE semantic.nyc_tlc__sem_daily_zone_trips
USING DELTA
COMMENT 'Daily yellow taxi trip KPIs by pickup zone'
AS
SELECT
    d.date_key,
    d.date,
    d.year,
    d.month,
    d.month_name,
    d.year_month,
    d.day_name,
    d.is_weekend,
    f.pu_location_id                                        AS pickup_location_id,
    COALESCE(z.borough, 'Unknown')                          AS pickup_borough,
    COALESCE(z.zone, 'Unknown')                             AS pickup_zone,
    COUNT(*)                                                AS trips,
    SUM(f.passenger_count)                                  AS passengers,
    ROUND(SUM(f.trip_distance), 2)                          AS total_distance_mi,
    ROUND(AVG(f.trip_distance), 2)                          AS avg_distance_mi,
    ROUND(AVG(f.trip_duration_min), 1)                      AS avg_duration_min,
    SUM(f.fare_amount)                                      AS total_fare,
    SUM(f.tip_amount)                                       AS total_tips,
    SUM(f.total_amount)                                     AS total_revenue,
    ROUND(AVG(f.total_amount), 2)                           AS avg_total_amount,
    ROUND(100 * SUM(CASE WHEN f.payment_type_id = 1 THEN f.tip_amount END)
              / NULLIF(SUM(CASE WHEN f.payment_type_id = 1 THEN f.fare_amount END), 0), 1)
                                                            AS tip_pct_of_fare,
    COUNT_IF(f.payment_type_id = 1)                         AS card_trips,
    COUNT_IF(f.payment_type_id = 2)                         AS cash_trips
FROM mart.nyc_tlc__fact_yellow_taxi_trips f
JOIN mart.nyc_tlc__dim_date d
  ON f.pickup_date_key = d.date_key
LEFT JOIN mart.nyc_tlc__dim_zone z
  ON f.pu_location_id = z.location_id
GROUP BY
    d.date_key, d.date, d.year, d.month, d.month_name, d.year_month, d.day_name, d.is_weekend,
    f.pu_location_id, z.borough, z.zone
""", target="semantic.nyc_tlc__sem_daily_zone_trips")

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
