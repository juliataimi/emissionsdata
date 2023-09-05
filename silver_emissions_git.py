# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

# Data from bronze
df = spark.read.table("bronze.emissions")
df2 = df.toPandas()

# COMMAND ----------

# raw data
df2.head()

# COMMAND ----------

# investigating dataset qualities
df2.country.nunique()

# COMMAND ----------

df2.dtypes

# COMMAND ----------

# Year to int 
# Value to float

df2['year'] = df2['year'].astype(str).astype(int)
df2['value'] = df2['value'].str.replace(',','.')
df2['value'] = df2['value'].astype(str).astype(float)
df2.dtypes

# COMMAND ----------

 # NaN values (not any)
 df2.isnull().values.any()

# COMMAND ----------

gas_filtered = df2[~df2["gas"].isin(["CO2", "N2O", "CH4"])]
#gas_filtered.head(3)

# COMMAND ----------

# Other gas types and values to one column as str 
# for example "HCFC: 333.444"

# Datatype float to str
gas_filtered['value'] = gas_filtered['value'].astype(float).astype(str)

# New column that contains "gas type: value"
gas_filtered['other_gas_and_value'] = gas_filtered['gas'] + ": " + gas_filtered['value']
#gas_filtered.head()

# COMMAND ----------

# Alternative gas types (not in {"CO2", "N2O", "CH4"})
gas_filtered['gas'].unique()

# COMMAND ----------

# Number of alternative rows
lkm = gas_filtered.shape[0]
print(f"There are {lkm} rows with alternative gas types")

# COMMAND ----------

# Number of countries in the dataframe 
df2.country.nunique()

# COMMAND ----------

# Testing that all countries have values in CO2, N2O, CH4
# Let's create a dataframe (to test) without alternative gas values

gases = {'CO2', 'N2O', 'CH4'}
df3 = df2[df2["gas"].isin(gases)]

#df3

# COMMAND ----------

# Number of countries after dropping alternative rows
df3.country.nunique()
# No countries were dropped = CO2, N2O and CH4 are the most relevant gas types

# COMMAND ----------

# Testing pivot with Dataframe that only has CO2, N2O and C4H gas types
# pivot > CO2, N2O, CH4 to columns 

gas_df = df3.pivot_table(index=['ISO', 'country', 'region_ar6_6', 'region_ar6_10', 'region_ar6_22', 'region_ar6_dev', 'year', 'sector_title', 'subsector_title'], columns='gas', values='value', aggfunc='first')

# Reset the index of the pivoted df
gas_df = gas_df.reset_index()


#gas_df.head()


# COMMAND ----------

gas_df.shape

# COMMAND ----------

# Finally one more dataframe with gas types and other gas types 
import pandas as pd
# Drop unnecessary columns (that were pivoted previously)
gas_filtered = gas_filtered.drop(['gas', 'value','gwp100_ar5'], axis=1)
# variable for concatenating
gases = [gas_df, gas_filtered]

# concatenating dataframes
emissions_df = pd.concat(gases)
emissions_df.head(3)

# COMMAND ----------

emissions_df = emissions_df.reset_index(drop=True)

# COMMAND ----------

emissions_df = emissions_df.drop_duplicates()

# COMMAND ----------

emissions_df.head()

# COMMAND ----------

isolist = emissions_df['ISO'].unique().tolist()

# COMMAND ----------

# MAGIC %run /Users/satu.haapakoski@brightstraining.com/country_code_function

# COMMAND ----------

emissions_df

# COMMAND ----------

testi_1_df = emissions_df.copy()
testi_1_df


# COMMAND ----------

print(testi_1_df.loc[1, 'country'])

# COMMAND ----------

country_info = {}

for code in isolist:
    country_info.update({code: get_country_name_from_code(code)})
print(country_info)



# COMMAND ----------

print(country_info["ABW"][0])

# COMMAND ----------

testi_1_df = testi_1_df.drop_duplicates()

# COMMAND ----------

# convert country_info dict to a dataframe
country_info_df = pd.DataFrame(country_info.values(), index=country_info.keys(), columns=['country', 'info', 'status'])

# merge dataframes based on ISO
merged_df = testi_1_df.merge(country_info_df, left_on='ISO', right_index=True, how='left')

# Use boolean indexing to update the 'country' column where status is not "404"
mask = merged_df['status'] != "404"
merged_df.loc[mask, 'country'] = merged_df.loc[mask, 'country_y']

# Drop unnecessary columns
merged_df = merged_df.drop(columns=['country_x', 'info', 'status', 'country_y'])

# Rename the DataFrame back to 'testi_1_df' if needed
testi_1_df = merged_df.copy()


# COMMAND ----------

testi_1_df

# COMMAND ----------

country_df = testi_1_df[['ISO','country', 'region_ar6_6', 'region_ar6_22', 'region_ar6_dev']]
country_df = country_df.rename(columns={"region_ar6_6": "region", "region_ar6_22": "subregion", "region_ar6_dev":"development_status"})
#country_df

# COMMAND ----------

country_df

# COMMAND ----------

#ensin korvataan maan nimet.
#iterointi, hidas
#tämä solu korvattiin toisella
'''
for i in range(len(testi_1_df)):
    key = testi_1_df['ISO'].iloc[i]
    if country_info[key][2] != "404":
        testi_1_df.loc[i, 'country'] = country_info[key][0]
'''


# COMMAND ----------

emissions_df = testi_1_df[['ISO','year', 'sector_title', 'subsector_title', 'CH4', 'CO2', 'N2O', 'other_gas_and_value']]
emissions_df

# COMMAND ----------

country_df = country_df.drop_duplicates()

# COMMAND ----------

country_df

# COMMAND ----------

emissionds_df.drop_duplicates()

# COMMAND ----------

# dataframes to spark
# emissions_df & country_df

spark_emissions = spark.createDataFrame(emissions_df) 
spark_emissions.printSchema()
#spark_emissions.show()

# COMMAND ----------

spark_country = spark.createDataFrame(country_df) 
spark_country.printSchema()

# COMMAND ----------

spark_emissions.write.mode("overwrite").saveAsTable("silver.emissions")

# COMMAND ----------

spark_country.write.mode("overwrite").saveAsTable("silver.country")

# COMMAND ----------


