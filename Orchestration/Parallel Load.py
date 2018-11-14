# Databricks notebook source
# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import java.sql.Timestamp
# MAGIC  sqlContext.udf.register("add_hours", (datetime : Timestamp, hours : Int) => {
# MAGIC     new Timestamp(datetime.getTime() + hours * 60 * 60 * 1000 )
# MAGIC });

# COMMAND ----------

def iterate_folders(path,filetoBeRead):
  folder = dbutils.fs.ls(path)
  for fname in folder:
    if fname.name.endswith(".gz"):
      #filetoBeRead.append(fname.path.replace(fname.name,"*.gz"))
      filetoBeRead.append(fname.path.replace(fname.name,"*.gz"))
      break
    elif fname.name.endswith("/"):
      #print(fname.path)
      iterate_folders(fname.path,filetoBeRead)


# COMMAND ----------

def load_files(filetoBeReadsite):
  for fname in filetoBeReadsite:
    startIndex = fname.find("/stage/") + 7
    endIndex = fname.find("/",startIndex) 
    siteCode = fname[startIndex:endIndex]
    startIndex =endIndex+1
    endIndex = fname.find("/",startIndex) 
    year = fname[startIndex:endIndex]
    startIndex =endIndex+1
    endIndex = fname.find("/",startIndex) 
    month=fname[startIndex:endIndex]
    startIndex =endIndex+1
    endIndex = fname.find("/",startIndex)
    day = fname[startIndex:endIndex].replace(year + "-" + month+"-","")
    cols,columnCount = get_column_list(fname)
    #print("processing "+ fname)
    transpose_stage_data(fname,siteCode,year,month,day,cols,columnCount)
    #print("processed "+ fname)

# COMMAND ----------

def get_column_list(path):
  sourceDf = spark.read.format("csv").option("header","true").option("delimiter", ",").load(path)
  fields = sourceDf.schema.fields

  cols =""
  columnCount =0

  for field in fields:
    columnCount+=1
    if columnCount>1:
      cols += "'"+field.name+"',`"+field.name+"`" if len(cols)==0 else "," + "'"+field.name+"',`"+field.name+"`"

  return cols,columnCount

# COMMAND ----------

dbutils.widgets.text("FilePath", "dbfs:/mnt/blobstorage/stage/", "EmptyLabel")
#print(dbutils.widgets.get("FilePath"))
fileDirectory = dbutils.widgets.get("FilePath")
filetoBeReadsite = list()
iterate_folders(fileDirectory,filetoBeReadsite)
load_files(filetoBeReadsite)
dbutils.notebook.exit(value)
#print getArgument("FilePath", "dbfs:/mnt/blobstorage/stage/aus001/2016")  


# COMMAND ----------

filetoBeRead = list()
filetoBeRead.append("dbfs:/mnt/blobstorage/stage/")
load_files(filetoBeRead)