# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

data = [
    ("UK", 2020, 320, 300, 67),
    ("UK", 2021, 330, 315, 67.2),
    ("UK", 2022, 340, 325, 67.5),
    ("India", 2020, 1500, 1400, 1391),
    ("India", 2021, 1580, 1470, 1393),
    ("India", 2022, 1650, 1550, 1396),
    ("Germany", 2020, 540, 500, 83),
    ("Germany", 2021, 530, 495, 83.2),
    ("Germany", 2022, 520, 490, 83.1)
]

schema = StructType([
    StructField("country", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("electricity_generation_twh", IntegerType(), True),
    StructField("electricity_demand_twh", IntegerType(), True),
    StructField("population_millions", DoubleType(), True)
])

df_raw = spark.createDataFrame(data, schema)

display(df_raw)



# COMMAND ----------

df_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_energy")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_energy;