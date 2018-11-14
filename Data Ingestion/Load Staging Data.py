# Databricks notebook source
# MAGIC %md 
# MAGIC #### Read Mapping data from config location and save as Parquet

# COMMAND ----------

spark.sql("USE GDPRAnalysis")
configDf = spark.read.format("csv").option("header","true").option("delimiter", ",").load("/mnt/blobstorage/config/dataitem_alias.csv")
configDf.write.mode('overwrite').insertInto("MappingData")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use DataMigration;
# MAGIC SELECT * FROM MappingData

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Read Site data from config location and save as Parquet

# COMMAND ----------

spark.sql("USE DataMigration")
configSiteDf = spark.read.format("csv").option("header","true").option("delimiter", ",").load("/mnt/blobstorage/config/sites.csv")
configSiteDf.write.mode('overwrite').insertInto("Site")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Timezone data from config location and save as Parquet

# COMMAND ----------

spark.sql("USE DataMigration")
configTZDf = spark.read.format("csv").option("header","true").option("delimiter", ",").load("/mnt/blobstorage/config/timezone.csv")
configTZDf.write.mode('overwrite').insertInto("Timezone")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use DataMigration;
# MAGIC select * from Timezone

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformation of stage data 
# MAGIC Load data from the source file and transpose the data to be used for Joins with Mapping Data

# COMMAND ----------

sourceDf = spark.read.format("csv").option("header","true").option("delimiter", ",").load("/mnt/blobstorage/stage/NLSE05/2018/09/05/*.gz")
fields = sourceDf.schema.fields

cols =""
columnCount =0

for field in fields:
  columnCount+=1
  if columnCount>1:
    cols += "'"+field.name+"',`"+field.name+"`" if len(cols)==0 else "," + "'"+field.name+"',`"+field.name+"`"
    
sourceCols = "stack("+str(columnCount)+"," + cols+") as (TagName,Value)"
#print sourceCols


transformedData = sourceDf.selectExpr("sample_dt", sourceCols).where("Value is not null")
transformedData.write.save("/mnt/datalake/NLSE05/2018/09/05/transformeddata05_4_2018-09-05_2018-09-06.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the transposed data to a Tables TransformedData

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS TransformedData
USING parquet
OPTIONS (
  path "{}"
)""".format("/mnt/datalake/NLSE05/2018/09/05/transformeddata05_4_2018-09-05_2018-09-06.parquet"))

# COMMAND ----------

spark.sql("""DROP TABLE TransformedDatav1""")
spark.sql("""
CREATE TABLE IF NOT EXISTS TransformedDatav1(EventTimeStamp string,DataItemId string,DataItemValue double,SiteCode string)
PARTITIONED BY (SiteCode)
CLUSTERED BY (DataItemId) INTO 256 BUCKETS
USING parquet
OPTIONS (
  path "{}"
)""".format("/mnt/datalake/parallelload/aus001/2016/01/01/transformeddata.parquet"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) FROM TransformedDatav1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Time series data to Data Lake 

# COMMAND ----------

timeSeriesData = spark.sql("""Select sample_dt AS EventTimeStamp,NewCode DataItemId,Value DataItemValue FROM TransformedData td
JOIN MappingData md ON td.TagName = md.ExistingTag""")
timeSeriesData.write.save("/mnt/datalake/NLSE05/2018/09/05/TimeSeriesData.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data from the parquet file to check if the data is Loaded Correctly

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW TimeSeriesData
USING parquet
OPTIONS (
  path "{}"
)""".format("/mnt/datalake/NLSE05/2018/09/05/transformeddata05_4_2018-09-05_2018-09-06.parquet"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TimeSeriesData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TimeSeriesData td
# MAGIC JOIN Timezone tz 
# MAGIC ON td.sample_dt between tz.StartDate and tz.EndDate
# MAGIC WHERE tz.TimeZone = 'NZ'