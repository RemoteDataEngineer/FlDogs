# Databricks notebook source
#import libraries
from pyspark.sql.functions import *
from delta.tables import *
import time
import datetime

# COMMAND ----------

# MAGIC %md 
# MAGIC # Record Initial Timestamp

# COMMAND ----------

begin_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Database

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS PetFinderDeltaLake
# MAGIC LOCATION '/mnt/dlsi2023/presentation'

# COMMAND ----------

# MAGIC %md
# MAGIC # Read in parquet files from the processed stage

# COMMAND ----------

FlDogs                  = spark.read.parquet("/mnt/dlsi2023/processed/fl_dogs.parquet")
FlDogsPhotos            = spark.read.parquet("/mnt/dlsi2023/processed/fl_dogs_photos.parquet")
FlDogsCroppedPhotos     = spark.read.parquet("/mnt/dlsi2023/processed/fl_cropped_dogs_photos.parquet")
FlOrganizations         = spark.read.parquet("/mnt/dlsi2023/processed/fl_organizations.parquet")
FlReferenceTags         = spark.read.parquet("/mnt/dlsi2023/processed/fl_reference_tags.parquet")

display(FlDogs)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add timestamp columns for Hive tables

# COMMAND ----------

FlDogs                  = FlDogs.withColumn("DeltaLakeRowCreated", current_timestamp().cast('timestamp')).withColumn("DeltaLakeRowModified", current_timestamp().cast('timestamp')).withColumn("ActiveIndicator", lit(1))

FlDogsPhotos            = FlDogsPhotos.withColumn("RowCreated", current_timestamp().cast('timestamp')).withColumn("RowModified", current_timestamp().cast('timestamp')).withColumn("ActiveIndicator", lit(1))


FlDogsCroppedPhotos     = FlDogsCroppedPhotos.withColumn("RowCreated", current_timestamp().cast('timestamp')).withColumn("RowModified", current_timestamp().cast('timestamp')).withColumn("ActiveIndicator", lit(1))


FlOrganizations         = FlOrganizations.withColumn("RowCreated", current_timestamp().cast('timestamp')).withColumn("RowModified", current_timestamp().cast('timestamp')).withColumn("ActiveIndicator", lit(1))

FlReferenceTags         = FlReferenceTags.withColumn("RowCreated", current_timestamp().cast('timestamp')).withColumn("RowModified", current_timestamp().cast('timestamp')).withColumn("ActiveIndicator", lit(1))

display(FlDogs)

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial Creation of other Hive tables to be refreshed

# COMMAND ----------

FlDogsPhotos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PetFinderDeltaLake.FlDogsPhotos")
FlDogsCroppedPhotos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PetFinderDeltaLake.FlDogsCroppedPhotos")
FlOrganizations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PetFinderDeltaLake.FlOrganizations")
FlReferenceTags.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PetFinderDeltaLake.FlReferenceTags")   

# COMMAND ----------

# MAGIC %md
# MAGIC # Creation and/or Upsert of Delta Lake table

# COMMAND ----------

#upsert of Delta lake

if spark.catalog.tableExists("PetFinderDeltaLake.FlDogs"):

    deltaTable = DeltaTable.forPath(spark, "/mnt/dlsi2023/presentation/fldogs/")

    deltaTable.alias('people').merge(FlDogs.alias('updates'),'people.AnimalsID = updates.AnimalsID') \
  .whenMatchedUpdate(set =
    {
        "AnimalsID": "updates.AnimalsID",
        "AnimalName": "updates.AnimalName",		
        "Age": "updates.Age",		
        "Description": "updates.Description",		
        "PrimaryBreed": "updates.PrimaryBreed",		
        "SecondaryBreed": "updates.SecondaryBreed",		
        "Mixed": "updates.Mixed",		
        "Unkown": "updates.Unkown",		
        "Declawed": "updates.Declawed",		
        "Housetrained": "updates.Housetrained",		
        "ShotsCurrent": "updates.ShotsCurrent",		
        "SpayedNeutered": "updates.SpayedNeutered",		
        "SpecialNeeds": "updates.SpecialNeeds",		
        "Coat": "updates.Coat",		
        "PrimaryColors": "updates.PrimaryColors",		
        "SecondaryColors": "updates.SecondaryColors",		
        "TertiaryColors": "updates.TertiaryColors",		
        "GoodWithChildren": "updates.GoodWithChildren",		
        "GoodWithCats": "updates.GoodWithCats",		
        "GoodWithDogs": "updates.GoodWithDogs",		
        "Published": "updates.Published",		
        "Size": "updates.Size",		
        "Status": "updates.Status",		
        "StatusChangedAt": "updates.StatusChangedAt",		
        "Type": "updates.Type",		
        "URL": "updates.URL",		
        "Address1": "updates.Address1",		
        "Address2": "updates.Address2",		
        "City": "updates.City",		
        "State": "updates.State",		
        "Postcode": "updates.Postcode",		
        "min_current_page": "updates.min_current_page",		
        "max_current_page": "updates.max_current_page",		
        "cnt": "updates.cnt",		
        "min_processed_date": "updates.min_processed_date",		
        "max_processed_date": "updates.max_processed_date",		
        "OrganizationId": "updates.OrganizationId",				
        "DeltaLakeRowModified": "updates.DeltaLakeRowModified",		
        "ActiveIndicator": "updates.ActiveIndicator"	
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "AnimalsID": "updates.AnimalsID",
        "AnimalName": "updates.AnimalName",		
        "Age": "updates.Age",		
        "Description": "updates.Description",		
        "PrimaryBreed": "updates.PrimaryBreed",		
        "SecondaryBreed": "updates.SecondaryBreed",		
        "Mixed": "updates.Mixed",		
        "Unkown": "updates.Unkown",		
        "Declawed": "updates.Declawed",		
        "Housetrained": "updates.Housetrained",		
        "ShotsCurrent": "updates.ShotsCurrent",		
        "SpayedNeutered": "updates.SpayedNeutered",		
        "SpecialNeeds": "updates.SpecialNeeds",		
        "Coat": "updates.Coat",		
        "PrimaryColors": "updates.PrimaryColors",		
        "SecondaryColors": "updates.SecondaryColors",		
        "TertiaryColors": "updates.TertiaryColors",		
        "GoodWithChildren": "updates.GoodWithChildren",		
        "GoodWithCats": "updates.GoodWithCats",		
        "GoodWithDogs": "updates.GoodWithDogs",		
        "Published": "updates.Published",		
        "Size": "updates.Size",		
        "Status": "updates.Status",		
        "StatusChangedAt": "updates.StatusChangedAt",		
        "Type": "updates.Type",		
        "URL": "updates.URL",		
        "Address1": "updates.Address1",		
        "Address2": "updates.Address2",		
        "City": "updates.City",		
        "State": "updates.State",		
        "Postcode": "updates.Postcode",		
        "min_current_page": "updates.min_current_page",		
        "max_current_page": "updates.max_current_page",		
        "cnt": "updates.cnt",		
        "min_processed_date": "updates.min_processed_date",		
        "max_processed_date": "updates.max_processed_date",		
        "OrganizationId": "updates.OrganizationId",				
        "DeltaLakeRowCreated": "updates.DeltaLakeRowCreated",
        "DeltaLakeRowModified": "updates.DeltaLakeRowModified",		
        "ActiveIndicator": "updates.ActiveIndicator"	
    }
  ).execute()
else:
    FlDogs.write.format("delta").mode("overwrite").partitionBy("ActiveIndicator").option("overwriteSchema", "true").saveAsTable("PetFinderDeltaLake.FlDogs")

# COMMAND ----------

# MAGIC %md
# MAGIC #Update ActiveIndicator to 0 if the row wasn't updated

# COMMAND ----------

#update ActiveIndicator to 0 if the row wasn't updated
deltaTable = DeltaTable.forPath(spark, '/mnt/dlsi2023/presentation/fldogs/')

deltaTable.update(
  condition = "DeltaLakeRowModified < date_sub(current_timestamp(),1)",
  set = { "ActiveIndicator": lit(0) }
)



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

log_df = spark.read.format("delta").load("/mnt/dlsi2023/presentation/fldogs/")  # query table by path

log_df = log_df.groupBy("DeltaLakeRowCreated", "DeltaLakeRowModified", "ActiveIndicator") \
 .agg(     
            count("*").alias("cnt")
    ).withColumn("PresentationLayerDone", current_timestamp().cast('timestamp'))

display(log_df )


# Check if the table exists in Hive
if spark.catalog.tableExists("PetFinderLogs.QCDeltaLake"):
  print("Table exists")
  log_df.write.mode("append").saveAsTable("PetFinderLogs.QCDeltaLake")
else:
  print("Table does not exist")
  log_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PetFinderLogs.QCDeltaLake")


# COMMAND ----------

# MAGIC %md
# MAGIC #Append Timestamps

# COMMAND ----------

end_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# The initial row of TimestampDF lists the current timestamp
initDF = [( 
           'Stage to Presentation - All Tables'
           ,  begin_timestamp 
           ,  end_timestamp 
           , 'Stage to Presentation Timestamps'
            )
        ]


dfColumns   = ["Timestamp","CreatedTimestamp","ModifiedTimestamp", "TimestampCategory"]
TimestampDF  = spark.createDataFrame(data=initDF, schema = dfColumns)

TimestampDF.write.mode("append").saveAsTable("PetFinderLogs.ETLLog")

# COMMAND ----------

# MAGIC %md
# MAGIC #Clean out raw and processed containers

# COMMAND ----------

# define function for removing files from a container
def empty_dir(dir_path, remove_dir=False):
  listFiles = dbutils.fs.ls(dir_path)
  for _file in listFiles:
    if _file.isFile():
      dbutils.fs.rm(_file.path)
  if remove_dir:
    dbutils.fs.rm(dir_path)

# COMMAND ----------

#call functions to clean out raw folder
empty_dir("/mnt/dlsi2023/raw/")
empty_dir("/mnt/dlsi2023/processed/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS fl_dogs.parquet;
# MAGIC DROP TABLE IF EXISTS fl_dogs_photos.parquet;
# MAGIC DROP TABLE IF EXISTS fl_cropped_dogs_photos.parquet;
# MAGIC DROP TABLE IF EXISTS fl_organizations.parquet;
# MAGIC DROP TABLE IF EXISTS fl_reference_tags.parquet;
