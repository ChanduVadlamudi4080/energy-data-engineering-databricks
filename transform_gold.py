# Databricks notebook source
df_silver = spark.table("silver_energy")
display(df_silver)


# COMMAND ----------

from pyspark.sql.functions import avg, sum, round

gold_country = df_silver.groupBy("country").agg(
    round(avg("generation_per_capita"), 2).alias("avg_generation_per_capita"),
    round(avg("sufficiency_ratio"), 2).alias("avg_sufficiency_ratio"),
    sum("energy_surplus_twh").alias("total_surplus_twh")
)

display(gold_country)


# COMMAND ----------

deficit_countries = gold_country.filter("avg_sufficiency_ratio < 1")

display(deficit_countries)


# COMMAND ----------

gold_country.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_energy_summary")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_energy_summary
# MAGIC ORDER BY avg_sufficiency_ratio DESC;
# MAGIC