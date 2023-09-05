# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

df = spark.read.table("bronze.world_population")
df2 = df.toPandas()

# COMMAND ----------

df2

# COMMAND ----------

df2=df2.drop(['_c3', '_c4'], axis=1)
df2

# COMMAND ----------

df2=df2.drop(['_c5', '_c6', '_c7', '_c9', '_c11', '_c12', '_c13'], axis=1)
df2

# COMMAND ----------

df2.rename(columns = {'_c0':'country','_c1':'year','_c2':'population', '_c8':'density','_c10':'urban_population'},inplace = True)
df2

# COMMAND ----------

df2.drop(df2.index[0], inplace=True) 
df2

# COMMAND ----------

df2.year.unique()

# COMMAND ----------

i = (df2.isna().any(axis=1))
print(df2[i])

# COMMAND ----------

df2["year"] = df2["year"].astype(int)

print(df2)
print(df2.dtypes)

# COMMAND ----------

df2.drop(df2[df2['year'] < 1990].index, inplace=True)
df2

# COMMAND ----------

df2.dtypes

# COMMAND ----------

df2["population"] = df2["population"].astype(int)
df2["density"] = df2["density"].astype(int)
print(df2)
print(df2.dtypes)

# COMMAND ----------

marks_list = df2['urban_population'].tolist()
  
# show the list
print(marks_list)

# COMMAND ----------

import numpy as np
df2.replace('N.A.', np.nan, inplace=True)

# COMMAND ----------

df2.dtypes

# COMMAND ----------

df2=df2.drop(['ingested_datetime', 'ingested_date'], axis=1)
df2

# COMMAND ----------

spark_df = spark.createDataFrame(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.world_population(
# MAGIC   country string,
# MAGIC   year int,
# MAGIC   population int,
# MAGIC   density int,
# MAGIC   urban_population int
# MAGIC )

# COMMAND ----------

spark_df.write.insertInto("silver.world_population")
