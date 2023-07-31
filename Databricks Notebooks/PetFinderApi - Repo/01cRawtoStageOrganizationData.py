# Databricks notebook source
# MAGIC %md
# MAGIC # Read in all pages of Organization data

# COMMAND ----------

#import libraries
from pyspark.sql import functions as F   
from pyspark.sql.functions import explode, col, lit, current_timestamp, sum,avg, min, max,count
import time
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Record initial timestamp

# COMMAND ----------

begin_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# MAGIC %md
# MAGIC # Import json files

# COMMAND ----------

# Read in all JSON files into one pyspark.sql.dataframe.DataFrame
organizations_df = spark.read.option("multiline","true").json('/mnt/dlsi2023/raw/ds_pet_finder_organization*.json')

# COMMAND ----------

# MAGIC %md
# MAGIC # Flatten struct and arrays into one level

# COMMAND ----------


#explode the array
flatten_organizations_df  = organizations_df.withColumn('organizations', explode('organizations'))

#flatten
format_organizations_df = flatten_organizations_df\
.withColumn("CurrentPage",F.col("pagination.current_page"))\
.withColumn("OrgID",F.col("organizations.id"))\
.withColumn("Name",F.col("organizations.name"))\
.withColumn("MissionStatement",F.col("organizations.mission_statement"))\
.withColumn("Address1",F.col("organizations.address.address1"))\
.withColumn("Address2",F.col("organizations.address.address2"))\
.withColumn("City",F.col("organizations.address.city"))\
.withColumn("State",F.col("organizations.address.state"))\
.withColumn("Postcode",F.col("organizations.address.postcode"))\
.withColumn("AdoptionPolicy",F.col("organizations.adoption.policy"))\
.withColumn("AdoptionURL",F.col("organizations.adoption.url"))\
.withColumn("Email",F.col("organizations.email"))\
.withColumn("facebook",F.col("organizations.social_media.facebook"))\
.withColumn("instagram",F.col("organizations.social_media.instagram"))\
.withColumn("twitter",F.col("organizations.social_media.twitter"))\
.withColumn("youtube",F.col("organizations.social_media.youtube"))\
.withColumn("pinterest",F.col("organizations.social_media.pinterest"))\
.withColumn("Sunday",F.col("organizations.hours.sunday"))\
.withColumn("Monday",F.col("organizations.hours.monday"))\
.withColumn("Tuesday",F.col("organizations.hours.tuesday"))\
.withColumn("Wednesday",F.col("organizations.hours.wednesday"))\
.withColumn("Thursday",F.col("organizations.hours.thursday"))\
.withColumn("Friday",F.col("organizations.hours.friday"))\
.withColumn("Saturday",F.col("organizations.hours.saturday"))\
.withColumn("ProcessedDate", current_timestamp())\
.drop('organizations')\
.drop('pagination')

# COMMAND ----------

# MAGIC %md
# MAGIC #transformation to one OrgID per line

# COMMAND ----------

#organizations are duplicated on different page
#transformation to one OrgID per line
group_organizations_df = format_organizations_df.groupBy("OrgID" ,"Name", "MissionStatement" , "Address1", "Address2", "City", "State","Postcode", "AdoptionPolicy", "AdoptionURL", "Email", "facebook", "instagram", "twitter", "youtube", "pinterest", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday") \
 .agg(      min("CurrentPage").alias("min_current_page"), \
            max("CurrentPage").alias("max_current_page"), \
            count("CurrentPage").alias("cnt"), \
            min("ProcessedDate").alias("min_processed_date"), \
            max("ProcessedDate").alias("max_processed_date")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC #Write to processed folder

# COMMAND ----------

#write to processed folder
group_organizations_df.write.partitionBy("City").mode("overwrite").parquet("/mnt/dlsi2023/processed/fl_organizations.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #Append Timestamps

# COMMAND ----------

end_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# The initial row of TimestampDF lists the current timestamp
initDF = [( 
           'RawtoStageFloridaOrganizations'
           ,  begin_timestamp 
           ,  end_timestamp 
           , 'Raw To Stage Timestamps'
            )
        ]


dfColumns   = ["Timestamp","CreatedTimestamp","ModifiedTimestamp", "TimestampCategory"]
TimestampDF  = spark.createDataFrame(data=initDF, schema = dfColumns)

TimestampDF .write.mode("append").saveAsTable("PetFinderLogs.ETLLog")

