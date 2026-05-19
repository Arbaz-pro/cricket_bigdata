# Databricks notebook source
# DBTITLE 1,import the required libarary
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,create catlog
spark.sql("create CATALOG if not exists workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.defauLT.cricket_api_project")
base_path='/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,calling api
API_KEY='f845a869-b4f8-4152-87cf-7a09467d87fd'
api_url=f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"
response=requests.get(api_url)
response.raise_for_status()

api_data=response.json()
print(json.dumps(api_data,indent=2)[:2000])

# COMMAND ----------

# DBTITLE 1,save raw api response
raw_file_path=f"{base_path}/current_matches_raw.json"

with open(raw_file_path,'w') as file:
    json.dump(api_data,file)

print("Raw api data is saved at :",raw_file_path)

# COMMAND ----------

# DBTITLE 1,create bronze_layer
bronze_data=[{
"source_api":api_url,
"raw_json":json.dumps(api_data),
"ingestion_time":None   
}]

bronze_schema=StructType([
StructField("source_api",StringType(),True),
StructField("raw_json",StringType(),True),
StructField("ingestion_time",TimestampType(),True)
])

bronze_df=spark.createDataFrame(bronze_data,schema=bronze_schema)\
    .withColumn("ingestion_time",current_timestamp())

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,store bronze
bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable('workspace.default.cricket_bronze_current_matches')

print("success")

# COMMAND ----------

print("done")

# COMMAND ----------


