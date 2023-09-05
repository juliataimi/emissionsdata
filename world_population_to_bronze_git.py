# Databricks notebook source
df = spark.read.csv(URL,KEY)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.world_population(
# MAGIC _c0 string,
# MAGIC _c1 string,
# MAGIC _c2 string,
# MAGIC _c3 string,
# MAGIC _c4 string,
# MAGIC _c5 string,
# MAGIC _c6 string,
# MAGIC _c7 string,
# MAGIC _c8 string,
# MAGIC _c9 string,
# MAGIC _c10 string,
# MAGIC _c11 string,
# MAGIC _c12 string,
# MAGIC _c13 string,
# MAGIC ingested_datetime timestamp,
# MAGIC ingested_date date
# MAGIC )
# MAGIC partitioned by (ingested_date);

# COMMAND ----------

from pyspark.sql import functions as F
bronze = df.withColumn("ingested_datetime", F.current_timestamp()).withColumn("ingested_date", F.current_date())


# COMMAND ----------

bronze.printSchema()

# COMMAND ----------

display(bronze)

# COMMAND ----------

bronze.write.insertInto("bronze.world_population")
