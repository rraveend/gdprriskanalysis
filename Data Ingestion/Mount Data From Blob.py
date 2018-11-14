# Databricks notebook source
# MAGIC %md
# MAGIC ##### Define shared methods for mount files from Azure Storage Blob

# COMMAND ----------

def mount_folder(source, extraConfigs, mountPoint) :
  try:
    dbutils.fs.mount(source=source,mount_point=mountPoint,extra_configs=extraConfigs)
    return True
  except:
    raise
  
      

# COMMAND ----------

def mount_exists(mountPoint) :
  try:
    files = dbutils.fs.ls(mountPoint)
    return True if len(files)>0 else False
  except:
    return False
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Defining variables required for mounting from blob
# MAGIC Currently these are hardcoded, to be changed in following sprint when the Keyvault is configured

# COMMAND ----------

#Storage account and key
storage_account_name = ""
storage_account_access_key = ""


# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount the staging folder and config

# COMMAND ----------

configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net" : storage_account_access_key}
#Mount Staging foder
if (mount_exists("/mnt/blobstorage/stage")==False):
  mount_folder("wasbs://<<stagefolder>>@"+storage_account_name+".blob.core.windows.net",configs,"/mnt/blobstorage/stage")
else:
  print("Mount name /mnt/blobstorage/stage already exists")
#Mount config foder from config container
if (mount_exists("/mnt/blobstorage/config")==False):
  mount_folder("wasbs://<<config>>@"+storage_account_name+".blob.core.windows.net",configs,"/mnt/blobstorage/config")
else:
  print("Mount name /mnt/blobstorage/config already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount the Data Lake Store

# COMMAND ----------

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "",
           "dfs.adls.oauth2.credential": "",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/<<TenantId>>/oauth2/token"}
if (mount_exists("/mnt/datalake")==False):
  mount_folder("adl://<<adlaccount>>.azuredatalakestore.net/sites",configs,"/mnt/datalake")
else:
  print("Mount name /mnt/datalake already exists")

# COMMAND ----------

