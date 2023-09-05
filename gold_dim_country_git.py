# Databricks notebook source
dim_country = spark.read.table("silver.country")
dim_country.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from silver.country
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table gold.dim_country

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.dim_country(
# MAGIC   country_code string,
# MAGIC   country string,
# MAGIC   region string,
# MAGIC   subregion string,
# MAGIC   development_status string
# MAGIC )

# COMMAND ----------

dim_country.write.insertInto("gold.dim_country")

# COMMAND ----------


