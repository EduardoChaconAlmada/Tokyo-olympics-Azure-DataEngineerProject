# Databricks notebook source

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "68e727d8-1b14-413b-9878-ab83393d5e4b",
"fs.azure.account.oauth2.client.secret": 'dEH8Q~US33AZkz6QsKEE5f5IHvorFRj_UGMeVahV',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/eb3c27e8-492a-4225-9c31-1ac5683742fe/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyoolympicdata@tokyoolympicsdb.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("/mnt/tokyoolymic/rawdata/Athletes.csv")
coaches = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("/mnt/tokyoolymic/rawdata/Coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("/mnt/tokyoolymic/rawdata/EntriesGender.csv")
medals = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("/mnt/tokyoolymic/rawdata/Medals.csv")
teams = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("/mnt/tokyoolymic/rawdata/Teams.csv")

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))




# COMMAND ----------

medals.show()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team/NOC","Gold").show()
     

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
avg_number_entries_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female']/entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male']/entriesgender['Total'])

avg_number_entries_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformeddata/athletes")
coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformeddata/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformeddata/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformeddata/medals")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformeddata/teams")

                                                                            
