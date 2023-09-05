# Databricks notebook source
spark.conf.set(URL,KEY)

# COMMAND ----------

df = spark.read.csv(FILE)
df.show(10)

# COMMAND ----------

import pandas as pd
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

df2.year.unique()

# COMMAND ----------

df1 = spark.read.csv("abfss://countryemissions@countryemissions.dfs.core.windows.net/rawdata/gdp.csv")
df1.show(10)

# COMMAND ----------

import pandas as pd
df3 = df1.toPandas()

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

df3

# COMMAND ----------

df3 = df3.reset_index(drop=True)
df3

# COMMAND ----------

df3 = pd.melt(df3,id_vars=df3.iloc[:,0:4], var_name='year', value_name='value')
df3

# COMMAND ----------

df3.year.unique()

# COMMAND ----------

df3=df3.drop(['Country Code', 'Indicator Name', 'Indicator Code'], axis=1)
df3

# COMMAND ----------

df3.rename(columns = {'value':'GDP'},inplace = True)
df3

# COMMAND ----------

i = (df3.isna().any(axis=1))
print(df3[i])

# COMMAND ----------

df3.rename(columns = {'Country Name':'country'},inplace = True)
df3

# COMMAND ----------

df3["year"] = df3["year"].astype(int)

print(df3)
print(df3.dtypes)

# COMMAND ----------

factcountryinfo = df2.merge(df3, on=['country', 'year'], how='inner')
factcountryinfo

# COMMAND ----------

factcountryinfo = factcountryinfo.reindex(columns=['country', 'year', 'GDP', 'population', 'density', 'urban_population'])
factcountryinfo

# COMMAND ----------

factcountryinfo['urban_population'].isnull().values.any()
