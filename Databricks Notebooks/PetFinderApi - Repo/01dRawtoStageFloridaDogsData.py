# Databricks notebook source
#import libraries
from pyspark.sql import functions as F   
from pyspark.sql.functions import explode, col, lit, current_timestamp, upper, ltrim, rtrim,concat, length, sum,avg, min, max,count
import time
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #Record initial timestamp

# COMMAND ----------

begin_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# MAGIC %md
# MAGIC # Import json files

# COMMAND ----------

# Read in all JSON files into one pyspark.sql.dataframe.DataFrame
fl_dogs_df = spark.read.option("multiline","true").json('/mnt/dlsi2023/raw/ds_pet_finder_fl_dog_*.json') 

# COMMAND ----------

# MAGIC %md
# MAGIC # Flatten struct and arrays into one level

# COMMAND ----------

#explode the animals array
flatten_fl_dogs_df  = fl_dogs_df.withColumn('animals', explode('animals'))

#flatten
format_fl_dogs_df = flatten_fl_dogs_df \
.withColumn("CurrentPage",F.col("pagination.current_page"))\
.withColumn("AnimalsID",F.col("animals.id"))\
.withColumn("OrganizationId",F.col("animals.organization_id"))\
.withColumn("AnimalName",F.col("animals.name"))\
.withColumn("Age",F.col("animals.age"))\
.withColumn("Description",F.col("animals.description"))\
.withColumn("PrimaryBreed",F.col("animals.breeds.primary"))\
.withColumn("SecondaryBreed",F.col("animals.breeds.secondary"))\
.withColumn("Mixed",F.col("animals.breeds.mixed"))\
.withColumn("Unkown",F.col("animals.breeds.unknown"))\
.withColumn("Declawed",F.col("animals.attributes.declawed"))\
.withColumn("Housetrained",F.col("animals.attributes.house_trained"))\
.withColumn("ShotsCurrent",F.col("animals.attributes.shots_current"))\
.withColumn("SpayedNeutered",F.col("animals.attributes.spayed_neutered"))\
.withColumn("SpecialNeeds",F.col("animals.attributes.special_needs"))\
.withColumn("Coat",F.col("animals.coat"))\
.withColumn("PrimaryColors",F.col("animals.colors.primary"))\
.withColumn("SecondaryColors",F.col("animals.colors.secondary"))\
.withColumn("TertiaryColors",F.col("animals.colors.tertiary"))\
.withColumn("Declawed",F.col("animals.attributes.house_trained"))\
.withColumn("GoodWithChildren",F.col("animals.environment.children"))\
.withColumn("GoodWithCats",F.col("animals.environment.cats"))\
.withColumn("GoodWithDogs",F.col("animals.environment.dogs"))\
.withColumn("Published",F.col("animals.published_at"))\
.withColumn("Size",F.col("animals.size"))\
.withColumn("Status",F.col("animals.status"))\
.withColumn("StatusChangedAt",F.col("animals.status_changed_at"))\
.withColumn("Type",F.col("animals.type"))\
.withColumn("URL",F.col("animals.url"))\
.withColumn("Address1",F.col("animals.contact.address.address1"))\
.withColumn("Address2",F.col("animals.contact.address.address2"))\
.withColumn("City",F.col("animals.contact.address.city"))\
.withColumn("State",F.col("animals.contact.address.state"))\
.withColumn("Postcode",F.col("animals.contact.address.postcode"))\
.withColumn("Tags",F.col("animals.tags"))\
.withColumn("PrimaryPhotoCropped",F.col("animals.primary_photo_cropped"))\
.withColumn("Videos",F.col("animals.videos"))\
.withColumn("Tags",F.col("animals.tags"))\
.withColumn("ProcessedDate", current_timestamp())\
.drop('animals')\
.drop('pagination')

# COMMAND ----------

# MAGIC %md
# MAGIC #transformation to one OrgID per line

# COMMAND ----------



#organizations are duplicated on different page
#transformation to one OrgID per line
group_fl_dogs_df = format_fl_dogs_df.groupBy("AnimalsID"
,"OrganizationId"
,"AnimalName"
,"Age"
,"Description"
,"PrimaryBreed"
,"SecondaryBreed"
,"Mixed"
,"Unkown"
,"Declawed"
,"Housetrained"
,"ShotsCurrent"
,"SpayedNeutered"
,"SpecialNeeds"
,"Coat"
,"PrimaryColors"
,"SecondaryColors"
,"TertiaryColors"
,"GoodWithChildren"
,"GoodWithCats"
,"GoodWithDogs"
,"Published"
,"Size"
,"Status"
,"StatusChangedAt"
,"Type"
,"URL"
,"Address1"
,"Address2"
,"City"
,"State"
,"Postcode") \
 .agg(      min("CurrentPage").alias("min_current_page"), \
            max("CurrentPage").alias("max_current_page"), \
            count("CurrentPage").alias("cnt"), \
            min("ProcessedDate").alias("min_processed_date"), \
            max("ProcessedDate").alias("max_processed_date")
    )
 

# COMMAND ----------

# MAGIC %md
# MAGIC # Flatten tags struct and arrays into one level

# COMMAND ----------

#flatten into new datataset
flatten_tags_fl_dogs_df = flatten_fl_dogs_df \
.withColumn("current_page",F.col("pagination.current_page"))\
.withColumn("AnimalsID",F.col("animals.id"))\
.withColumn("AnimalName",F.col("animals.name"))\
.withColumn("Age",F.col("animals.age"))\
.withColumn("Description",F.col("animals.description"))\
.withColumn("PrimaryBreed",F.col("animals.breeds.primary"))\
.withColumn("Tags",F.col("animals.tags"))\
.withColumn("processed_date", current_timestamp())\
.drop('animals')\
.drop('pagination')

#explode the tags array
format_tags_fl_dogs = flatten_tags_fl_dogs_df.withColumn('tags', explode('tags'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Flatten photo struct and arrays into one level

# COMMAND ----------

#flatten into new datataset
flatten_photos_fl_dogs_df = flatten_fl_dogs_df \
.withColumn("current_page",F.col("pagination.current_page"))\
.withColumn("AnimalsID",F.col("animals.id"))\
.withColumn("AnimalName",F.col("animals.name"))\
.withColumn("Age",F.col("animals.age"))\
.withColumn("Description",F.col("animals.description"))\
.withColumn("PrimaryBreed",F.col("animals.breeds.primary"))\
.withColumn("Photos",F.col("animals.photos"))\
.withColumn("processed_date", current_timestamp())\
.drop('animals')\
.drop('pagination')

#explode the tags array
format_photos_fl_dogs = flatten_photos_fl_dogs_df.withColumn('photos', explode('photos'))

format_photos_fl_dogs = format_photos_fl_dogs.withColumn("full_photo", F.col("photos.full")).withColumn("large_photo", F.col("photos.large")).withColumn("medium_photo", F.col("photos.medium")).withColumn("small_photo", F.col("photos.small")).drop('photos')


# COMMAND ----------

# MAGIC %md
# MAGIC # Flatten cropped photo struct and arrays into one level

# COMMAND ----------

#flatten into new datataset
flatten_cropped_photos_fl_dogs_df = flatten_fl_dogs_df \
.withColumn("current_page",F.col("pagination.current_page"))\
.withColumn("AnimalsID",F.col("animals.id"))\
.withColumn("AnimalName",F.col("animals.name"))\
.withColumn("Age",F.col("animals.age"))\
.withColumn("Description",F.col("animals.description"))\
.withColumn("PrimaryBreed",F.col("animals.breeds.primary"))\
.withColumn("PrimaryCroppedPhotosFull",F.col("animals.primary_photo_cropped.full"))\
.withColumn("PrimaryCroppedPhotosLarge",F.col("animals.primary_photo_cropped.large"))\
.withColumn("PrimaryCroppedPhotosMedium",F.col("animals.primary_photo_cropped.medium"))\
.withColumn("PrimaryCroppedPhotosSmall",F.col("animals.primary_photo_cropped.small"))\
.withColumn("processed_date", current_timestamp())\
.drop('animals')\
.drop('pagination')


# COMMAND ----------

# MAGIC %md
# MAGIC # Reference tables

# COMMAND ----------

#put tags in a reference table for Power Bi
reference_tags = format_tags_fl_dogs.select( ltrim(upper(col("Tags")  )  ).alias("Tags") ).dropDuplicates(["Tags"]).orderBy(['Tags'],ascending=True).where("length(Tags) > 0")


# COMMAND ----------

#write to process folder
group_fl_dogs_df.write.partitionBy("OrganizationId").mode("overwrite").parquet("/mnt/dlsi2023/processed/fl_dogs.parquet")
reference_tags.write.mode("overwrite").parquet("/mnt/dlsi2023/processed/fl_reference_tags.parquet")
flatten_cropped_photos_fl_dogs_df.write.mode("overwrite").parquet("/mnt/dlsi2023/processed/fl_cropped_dogs_photos.parquet") 
format_photos_fl_dogs.write.mode("overwrite").parquet("/mnt/dlsi2023/processed/fl_dogs_photos.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Append Timestamps

# COMMAND ----------

end_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# The initial row of TimestampDF lists the current timestamp
initDF = [( 
           'RawtoStageFloridaDogs'
           ,  begin_timestamp 
           ,  end_timestamp 
           , 'Raw To Stage Timestamps'
            )
        ]


dfColumns   = ["Timestamp","CreatedTimestamp","ModifiedTimestamp", "TimestampCategory"]
TimestampDF  = spark.createDataFrame(data=initDF, schema = dfColumns)

TimestampDF .write.mode("append").saveAsTable("PetFinderLogs.ETLLog")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PetFinderLogs.ETLLog order by ModifiedTimestamp;
