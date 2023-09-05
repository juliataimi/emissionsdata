# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

df = spark.read.table("bronze.gdp")
df3 = df.toPandas()

# COMMAND ----------

df3

# COMMAND ----------

df3=df3.drop(['_c4', '_c5', '_c6', '_c7', '_c8', '_c9', '_c10'], axis=1)
df3

# COMMAND ----------

df3=df3.drop(['_c11', '_c12', '_c13', '_c14', '_c15', '_c16', '_c17', '_c18', '_c19', '_c20', '_c21', '_c22', '_c23', '_c24', '_c25', '_c26', '_c27', '_c28', '_c29', '_c30', '_c31', '_c32', '_c33', '_c67'], axis=1)
df3

# COMMAND ----------

df3.columns = df3.iloc[0]
df3 = df3[1:]

# COMMAND ----------

df3 = df3.reset_index(drop=True)
df3

# COMMAND ----------

df3 = pd.melt(df3,id_vars=df3.iloc[:,0:4], var_name='year', value_name='value')
df3

# COMMAND ----------

df3=df3.drop(['Indicator Name', 'Indicator Code'], axis=1)
df3

# COMMAND ----------

df3.rename(columns = {'value':'GDP'},inplace = True)
df3

# COMMAND ----------

df3["year"] = df3["year"].astype(int)

print(df3)
print(df3.dtypes)

# COMMAND ----------

df3.rename(columns = {'Country Name':'country'},inplace = True)
df3

# COMMAND ----------

df3.rename(columns = {'Country Code':'ISO'}, inplace = True)
df3

# COMMAND ----------

spark_df1 = spark.createDataFrame(df3)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists silver.gdp(
# MAGIC   country string,
# MAGIC   ISO string,
# MAGIC   year int,
# MAGIC   GDP string
# MAGIC )

# COMMAND ----------

spark_df1.write.insertInto("silver.gdp")
