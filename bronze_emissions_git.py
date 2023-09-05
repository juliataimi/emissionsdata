# Databricks notebook source
spark.conf.set(URL,KEY)

# COMMAND ----------

# Reading the emission data to a dataframe

df = spark.read.option("header", "true").option("sep", ";").csv(FILE)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.emissions(
# MAGIC   ISO string, 
# MAGIC   country string,
# MAGIC   region_ar6_6 string,
# MAGIC   region_ar6_10 string, 
# MAGIC   region_ar6_22 string,
# MAGIC   region_ar6_dev string,
# MAGIC   year string,
# MAGIC   sector_title string,
# MAGIC   subsector_title string,
# MAGIC   gas string,
# MAGIC   gwp100_ar5 string,
# MAGIC   value string,
# MAGIC   ingested_date date
# MAGIC )
# MAGIC partitioned by (ingested_date)

# COMMAND ----------

from pyspark.sql import functions as F
bronze_emissions = df.withColumn("ingested_date", F.current_date())

# COMMAND ----------

bronze_emissions.write.insertInto("bronze.emissions")

# COMMAND ----------

# bronze_emission to a function
# toPandas 
# call it in the silver notebook

def bronze_emission():
    bronze_emission = df.toPandas()
    return bronze_emission

# COMMAND ----------

### Data is now ingested to bronze_emission table

# COMMAND ----------

bronze_emission()

# COMMAND ----------


