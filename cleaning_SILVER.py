# Databricks notebook source
df_bronze = spark.table("bronze_energy")
display(df_bronze)


# COMMAND ----------

from pyspark.sql.functions import col, round

df_silver = df_bronze \
    .withColumn("energy_surplus_twh",
                col("electricity_generation_twh") - col("electricity_demand_twh")) \
    .withColumn("generation_per_capita",
                round(col("electricity_generation_twh") / col("population_millions"), 2)) \
    .withColumn("sufficiency_ratio",
                round(col("electricity_generation_twh") / col("electricity_demand_twh"), 2))

display(df_silver)


# COMMAND ----------

df_silver = df_silver.filter(
    (col("electricity_generation_twh") > 0) &
    (col("electricity_demand_twh") > 0)
)


# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_energy")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_energy;