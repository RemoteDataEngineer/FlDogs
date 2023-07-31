# Databricks notebook source
#import libraries
import os
import time
import datetime
from pyspark.sql.functions import col, concat, current_timestamp, lit, when
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.catalog import *


# COMMAND ----------

# MAGIC %md
# MAGIC # Create dataframe of all ingestion timestamps

# COMMAND ----------

# Path to the file/directory
path = "/dbfs/mnt/dlsi2023/raw/"


# The initial row of TimestampDF lists the current timestamp
initDF = [( 
           'Ingestion Timestamps'
           ,  datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
           ,  datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
           , 'Ingestion'
            )
        ]


dfColumns   = ["Timestamp","CreatedTimestamp","ModifiedTimestamp", "TimestampCategory"]
IngestionTimestampDF  = spark.createDataFrame(data=initDF, schema = dfColumns)

#for loop through filenames in path
for file_item in os.listdir(path):
    file_path = os.path.join(path, file_item) #construct file path string
    ti_c = os.path.getctime(file_path) #timestamp created
    ti_m = os.path.getmtime(file_path) #timestamp modified
        
    c_ti = str(datetime.datetime.fromtimestamp(ti_c).strftime('%Y-%m-%d %H:%M:%S'))#format timestamp created
    m_ti = str(datetime.datetime.fromtimestamp(ti_m).strftime('%Y-%m-%d %H:%M:%S'))#format timestamp modified
    
    #populate list with variables initialized at begnning of for loop
    indTimeStampsDF = [( 
        file_item
        ,  c_ti
        ,  m_ti 
        ,  file_item
     )
    ]

    #create a new dataframe row from list and union it to TimestampDF
    newRow = spark.createDataFrame(indTimeStampsDF  , schema = dfColumns )
    IngestionTimestampDF = IngestionTimestampDF.union(newRow)



# COMMAND ----------

# MAGIC %md
# MAGIC #Update Modified Timestamp TimestampDF to reflect current timestamp

# COMMAND ----------

#Update Modified Timestamp with Current Timestamp
IngestionTimestampDF = IngestionTimestampDF.withColumn('ModifiedTimestamp', when(col('TimestampCategory').contains('Ingestion'), datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')).otherwise(col('ModifiedTimestamp') ))

#Add Current Timestamp to Fl Dogs Ingestion
IngestionTimestampDF = IngestionTimestampDF.withColumn('TimestampCategory', when(col('Timestamp').contains("ds_pet_finder_fl"),  "Read In Pages of FL Dogs").otherwise(col('TimestampCategory')))

#Add Current Timestamp to Fl Organizations Timestamp
IngestionTimestampDF = IngestionTimestampDF.withColumn('TimestampCategory', when(col('Timestamp').contains("organization"),  "Read In Pages of FL Organizations").otherwise(col('TimestampCategory')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Add / Update PetFinderLogs

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS PetFinderLogs
# MAGIC LOCATION '/mnt/dlsi2023/logtables'
# MAGIC

# COMMAND ----------


# Check if the table exists in Hive
if spark.catalog.tableExists("PetFinderLogs.ETLLog"):
  print("Table exists")
  IngestionTimestampDF.write.mode("append").saveAsTable("PetFinderLogs.ETLLog")
else:
  print("Table does not exist")
  IngestionTimestampDF.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PetFinderLogs.ETLLog")

