# energy-data-engineering-databricks
This project demonstrates a full data engineering pipeline using Databricks + Delta Lake, implementing the Medallion Architecture (Bronze → Silver → Gold → Insight).

Goal: Clean, transform, and analyze energy datasets to generate business-ready insights.

Key Skills Showcased:

Python + PySpark

Delta Lake table creation & transformation

SQL aggregation and analytics

Medallion Architecture (Bronze → Silver → Gold → Insight)

ETL & data quality enforcement

Project Architecture
Bronze (raw)  →  Silver (cleaned + enriched)  →  Gold (aggregated)  →  Insight (SQL views)


Diagram Placeholder:


Bronze: Raw dataset ingestion

Silver: Clean + derived metrics

Gold: Aggregated insights

Insight: SQL queries for business analytics

| Column                       | Description                   |
| ---------------------------- | ----------------------------- |
| `country`                    | Country name                  |
| `year`                       | Year of record                |
| `electricity_generation_twh` | Electricity generation in TWh |
| `electricity_demand_twh`     | Electricity demand in TWh     |
| `population_millions`        | Population in millions        |




