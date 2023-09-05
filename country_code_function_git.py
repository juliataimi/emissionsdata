# Databricks notebook source
import requests
import json

def get_country_code_from_name(country):
    country_lower = country.lower()
    url = "https://restcountries.com/v3.1/name/"+ country +"?fullText=true"

    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = json.loads(response.text)
        country_code = data[0]["cca2"]
        return country_code
    else:
        return country + " not found " + str(response.status_code)

# COMMAND ----------

def get_country_from_name_code(country,code):
    result = []
    url = "https://restcountries.com/v3.1/alpha/" + code
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = json.loads(response.text)
        code = data[0]["cca2"]
        country = data[0]["name"]["common"]
    else:
        code = "not found " + str(response.status_code)
    result = [country,code]
    return result

# COMMAND ----------

def get_common_country_name(country):
    country_lower = country.lower()
    country_lower = country_lower.replace(",","")
    url = "https://restcountries.com/v3.1/name/"+ country +"?fullText=true"

    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = json.loads(response.text)
        country = data[0]["name"]["common"]
    else: 
        country = country + " not found " + str(response.status_code)
    
    return country

# COMMAND ----------

def get_country_name_from_code(code1):
    result = []
    url = "https://restcountries.com/v3.1/alpha/" + code1
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = json.loads(response.text)
        code1 = data[0]["cca3"]
        code2 = data[0]["cca2"]
        country = data[0]["name"]["common"]
    else:
        country = "not found "
        code2 = str(response.status_code)
    result = [country,code1,code2]
    return result
