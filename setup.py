# Databricks notebook source
# MAGIC %md
# MAGIC This file shoud be run from 'Delta + ML Rossmann' notebook. It mounts the dbfs on the container and performs a table cleanup for multiple runs.

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
logging.getLogger('py4j').setLevel(logging.ERROR)
from pyspark.sql.functions import to_date, col
import tempfile
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name
import re
import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# DBTITLE 1,Blob Storage Configuration
storage_account_name = "rossmann"
storage_account_access_key = dbutils.secrets.get(scope = "demo", key = "rossmann")
file_type = "csv"
container_name = "raw-data"
mount_name = "/mnt/rossmann"

# COMMAND ----------

# DBTITLE 1,Mount DBFS on the Blob Container
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbName = re.sub(r'\W+', '_', current_user)
path = "/Users/{}/demo".format(current_user)
dbutils.widgets.text("path", path, "path")
dbutils.widgets.text("dbName", dbName, "dbName")
print("using path {}".format(path))
spark.sql("""create database if not exists {} LOCATION '{}/global_demo/tables' """.format(dbName, path))
spark.sql("""USE {}""".format(dbName))

def mount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        return
    else: 
      dbutils.fs.mount(
      source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net",
      mount_point = str_path,
      extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":dbutils.secrets.get(scope = "demo", key = "rossmann")})
    return
      
mount(mount_name)

# COMMAND ----------

#Allow schema inference for auto loader
dbutils.fs.rm(path+"/retail/_checkpoint", True)
spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Clean-up Tables Before the Run
spark.sql("""drop table if exists tania.rossmann_sales""".format(dbName))
dbutils.fs.rm("dbfs:/user/hive/warehouse/tania.db/rossmann_sales", True)

spark.sql("""drop table if exists tania.bronze_rossmann_sales""".format(dbName))
dbutils.fs.rm("dbfs:/user/hive/warehouse/tania.db/bronze_rossmann_sales", True)

spark.sql("""drop table if exists tania.silver_rossmann_sales""".format(dbName))
dbutils.fs.rm("dbfs:/user/hive/warehouse/tania.db/silver_rossmann_sales", True)
dbutils.fs.rm("/mnt/rossmann/silver", True)

dbutils.fs.rm("dbfs:/user/hive/warehouse/tania.db/silver_rossmann_ml", True)
dbutils.fs.rm("/mnt/rossmann/silver_ml", True)

spark.sql("""drop table if exists tania.gold_rossmann_sales""".format(dbName))
dbutils.fs.rm("dbfs:/user/hive/warehouse/tania.db/gold_rossmann_sales", True)
dbutils.fs.rm("/mnt/rossmann/gold", True)


