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

# # nb_05_nyc_tlc_semantic (mart → semantic, Spark SQL)
# `semantic.nyc_tlc__sem_daily_zone_trips`: one row per **day × pickup zone**, already joined to the date and zone attributes, so it's ready for a report or a Direct Lake semantic model.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS semantic

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE semantic.nyc_tlc__sem_daily_zone_trips
# MAGIC USING DELTA
# MAGIC COMMENT 'Daily yellow taxi trip KPIs by pickup zone'
# MAGIC AS
# MAGIC SELECT
# MAGIC     d.date_key,
# MAGIC     d.date,
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     d.year_month,
# MAGIC     d.day_name,
# MAGIC     d.is_weekend,
# MAGIC     f.pu_location_id                                AS pickup_location_id,
# MAGIC     COALESCE(z.borough, 'Unknown')                  AS pickup_borough,
# MAGIC     COALESCE(z.zone, 'Unknown')                     AS pickup_zone,
# MAGIC     COUNT(*)                                        AS trips,
# MAGIC     SUM(f.passenger_count)                          AS passengers,
# MAGIC     ROUND(SUM(f.trip_distance), 2)                  AS total_distance_mi,
# MAGIC     ROUND(AVG(f.trip_distance), 2)                  AS avg_distance_mi,
# MAGIC     ROUND(AVG(f.trip_duration_min), 1)              AS avg_duration_min,
# MAGIC     SUM(f.fare_amount)                              AS total_fare,
# MAGIC     SUM(f.tip_amount)                               AS total_tips,
# MAGIC     SUM(f.total_amount)                             AS total_revenue,
# MAGIC     ROUND(AVG(f.total_amount), 2)                   AS avg_total_amount,
# MAGIC     ROUND(100 * SUM(CASE WHEN f.payment_type_id = 1 THEN f.tip_amount END)
# MAGIC               / NULLIF(SUM(CASE WHEN f.payment_type_id = 1 THEN f.fare_amount END), 0), 1)
# MAGIC                                                     AS tip_pct_of_fare,
# MAGIC     COUNT_IF(f.payment_type_id = 1)                 AS card_trips,
# MAGIC     COUNT_IF(f.payment_type_id = 2)                 AS cash_trips
# MAGIC FROM mart.nyc_tlc__fact_yellow_taxi_trips f
# MAGIC JOIN mart.nyc_tlc__dim_date d
# MAGIC   ON f.pickup_date_key = d.date_key
# MAGIC LEFT JOIN mart.nyc_tlc__dim_zone z
# MAGIC   ON f.pu_location_id = z.location_id
# MAGIC GROUP BY
# MAGIC     d.date_key, d.date, d.year, d.month, d.month_name, d.year_month, d.day_name, d.is_weekend,
# MAGIC     f.pu_location_id, z.borough, z.zone

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT year_month, pickup_borough, SUM(trips) AS trips, ROUND(SUM(total_revenue), 0) AS revenue
# MAGIC FROM semantic.nyc_tlc__sem_daily_zone_trips
# MAGIC GROUP BY year_month, pickup_borough
# MAGIC ORDER BY year_month, trips DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
