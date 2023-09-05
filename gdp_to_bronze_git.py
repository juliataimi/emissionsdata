# Databricks notebook source
spark.conf.set(URL,
    "azure account key")

# COMMAND ----------

df = spark.read.csv("file.csv")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("bronze.gdp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.gdp

# COMMAND ----------


