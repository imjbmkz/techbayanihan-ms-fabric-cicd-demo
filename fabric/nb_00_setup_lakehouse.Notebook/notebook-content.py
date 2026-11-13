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

# CELL ********************

# Idempotent: safe to run in every environment, every time.
for schema in ["raw", "cleaned", "mart", "semantic", "ops"]:  # ops = run logs / audit
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW SCHEMAS"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# "data lake" layer = Files area, acts like the S3 landing zone
for folder in ["datalake/nyc_tlc/yellow_taxi_trips", "datalake/nyc_tlc/taxi_zones", "logs"]:
    notebookutils.fs.mkdirs(f"Files/{folder}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
