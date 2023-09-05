# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

df = spark.read.table("silver.world_population")
df2 = df.toPandas()

# COMMAND ----------

df2

# COMMAND ----------

df = spark.read.table("silver.gdp")
df3 = df.toPandas()

# COMMAND ----------

df3

# COMMAND ----------

df3.year.unique()

# COMMAND ----------

df2.year.unique()

# COMMAND ----------

factcountryinfo = df2.merge(df3, on=['country', 'year'], how='inner')
factcountryinfo

# COMMAND ----------

factcountryinfo=factcountryinfo.drop(['country'], axis=1)
factcountryinfo

# COMMAND ----------

factcountryinfo = factcountryinfo.reindex(columns=['ISO', 'year', 'GDP', 'population', 'density', 'urban_population'])
factcountryinfo

# COMMAND ----------

# MAGIC %sql
# MAGIC select 

# COMMAND ----------

factcountryinfo.insert(0, 'info_id', range(1, 1 + len(factcountryinfo)))
factcountryinfo

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.FactCountryInfo(
# MAGIC   info_id int, 
# MAGIC   country_code string,
# MAGIC   year int,
# MAGIC   GDP decimal (18,1),
# MAGIC   population int,
# MAGIC   density int,
# MAGIC   urban_population int
# MAGIC )

# COMMAND ----------

spark_df = spark.createDataFrame(factcountryinfo)

# COMMAND ----------

spark_df.write.insertInto("gold.FactCountryInfo")
