# Databricks notebook source
# MAGIC %run "./parallel-notebooks"

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.concurrent.Await
# MAGIC import scala.concurrent.duration._
# MAGIC import scala.language.postfixOps
# MAGIC 
# MAGIC val notebooks = Seq(
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file001")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file002")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file003")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file004")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file005")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file006")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file007")),
# MAGIC   NotebookData("Parallel Load", 0, Map("FilePath" -> "dbfs:/mnt/blobstorage/stage/file008"))
# MAGIC )
# MAGIC 
# MAGIC val res = parallelNotebooks(notebooks)
# MAGIC 
# MAGIC Await.result(res, 40000 seconds) // wait for 10 hours
# MAGIC res.value

# COMMAND ----------

