# Databricks notebook source
fact_emissions = spark.read.table("silver.emissions")
fact_emissions.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from silver.emissions

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.fact_emissions(
# MAGIC   country_code string,
# MAGIC   year int,
# MAGIC   sector_title string,
# MAGIC   subsector_title string,
# MAGIC   CH4 double,
# MAGIC   CO2 double,
# MAGIC   N2O double, 
# MAGIC   other_gas_and_value string
# MAGIC )

# COMMAND ----------

fact_emissions.write.insertInto("gold.fact_emissions")

# COMMAND ----------


