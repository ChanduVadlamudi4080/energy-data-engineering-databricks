# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM gold_energy_summary;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country,
# MAGIC        SUM(electricity_generation_twh) AS total_generation,
# MAGIC        SUM(electricity_demand_twh) AS total_demand
# MAGIC FROM silver_energy
# MAGIC GROUP BY country
# MAGIC ORDER BY total_generation DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country,
# MAGIC        SUM(electricity_generation_twh - electricity_demand_twh) AS surplus_deficit
# MAGIC FROM silver_energy
# MAGIC GROUP BY country
# MAGIC HAVING SUM(electricity_generation_twh - electricity_demand_twh) < 0;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country,
# MAGIC        ROUND(AVG(electricity_generation_twh / population_millions), 2) AS avg_generation_per_capita
# MAGIC FROM silver_energy
# MAGIC GROUP BY country
# MAGIC ORDER BY avg_generation_per_capita DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country,
# MAGIC        year,
# MAGIC        electricity_generation_twh - electricity_demand_twh AS surplus_deficit
# MAGIC FROM silver_energy
# MAGIC ORDER BY country, year;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country,
# MAGIC        ROUND(SUM(electricity_generation_twh)/SUM(electricity_demand_twh), 2) AS sufficiency_ratio
# MAGIC FROM silver_energy
# MAGIC GROUP BY country
# MAGIC ORDER BY sufficiency_ratio DESC
# MAGIC LIMIT 1;
# MAGIC