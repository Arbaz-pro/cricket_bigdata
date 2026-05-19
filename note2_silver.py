# Databricks notebook source
# DBTITLE 1,import

import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_df=spark.table('workspace.default.cricket_bronze_current_matches')
raw_json=bronze_df.select("raw_json").collect()[0]['raw_json']
api_data=json.loads(raw_json)

matches=api_data.get("data",[])

print("total matches: ",len(matches))
print(matches[0] if len(matches)>0 else "no matches")

# COMMAND ----------

# DBTITLE 1,extract fields
silver_rows=[]


# COMMAND ----------



if len(score)>0:
    score_1=f"{score[0].get("r",0)}/{score[0].get("w",0)} in {score[0].get("o",0)} overs"
    score_2=f"{score[1].get("r",0)}/{score[1].get("w",0)} in {score[1].get("o",0)} overs"
    print(score_1)
    print(score_2)


# COMMAND ----------

for match in matches:
    teams=match.get("teams",[])
    score=match.get("score",[])
    team_1=teams[0] if len(teams)>0 else None
    team_2=teams[1] if len(teams)>0 else None
    if len(score)>0:
        score_1=f"{score[0].get("r",0)}/{score[0].get("w",0)} in {score[0].get("o",0)} overs"
        score_2=f"{score[1].get("r",0)}/{score[1].get("w",0)} in {score[1].get("o",0)} overs"
    silver_rows.append({
        "match_id": match.get("id"),
        "match_name": match.get("name"),
        "match_type": match.get("matchType"),
        "status": match.get("status"),
        "venue": match.get("venue"),
        "match_date": match.get("date"),
        "date_time_gmt": match.get("dateTimeGMT"),
        "team_1": team_1,
        "team_2": team_2,
        "score_1": score_1,
        "score_2": score_2,
        "match_started": match.get("matchStarted"),
        "match_ended": match.get("matchEnded")
    })
print("silver_rows prepared", len(silver_rows))

# COMMAND ----------

print(silver_rows[0])

# COMMAND ----------

silver_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("match_name", StringType(), True),
    StructField("match_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("date_time_gmt", StringType(), True),
    StructField("team_1", StringType(), True),
    StructField("team_2", StringType(), True),
    StructField("score_1", StringType(), True),
    StructField("score_2", StringType(), True),
    StructField("match_started", BooleanType(), True),
    StructField("match_ended", BooleanType(), True)
])

silver_df=spark.createDataFrame(silver_rows,schema=silver_schema)\
    .withColumn("match_date",to_date(col("match_date")))\
    .withColumn("loaded_at",current_timestamp())

display(silver_df)


# COMMAND ----------

silver_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.cricket_silver_current_matches")
print("done")

# COMMAND ----------


