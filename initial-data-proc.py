# Databricks notebook source
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

# COMMAND ----------

storage_account_name = "rossmann"
storage_account_access_key = dbutils.secrets.get(scope = "demo", key = "rossmann")
file_type = "csv"
container_name = "raw-data"
mount_name = "/mnt/rossmann"

# COMMAND ----------

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

# MAGIC %fs ls /mnt/rossmann

# COMMAND ----------

df_1 = pv.read_csv("/dbfs/mnt/rossmann/train.csv")
df_1

# COMMAND ----------

df_1["StateHoliday"]

# COMMAND ----------

fields = [
    pa.field('Store', pa.int64()),
    pa.field('DayOfWeek', pa.int64()),
    pa.field('Date', pa.timestamp('ms')),
    pa.field('Sales', pa.int64()),
    pa.field('Customers', pa.int64()),
    pa.field('Open', pa.int64()),
    pa.field('Promo', pa.int64()),
    pa.field('StateHoliday', pa.int64()),
    pa.field('SchoolHoliday', pa.int64()),
]

my_schema = pa.schema(fields)

# COMMAND ----------


columns = []
my_columns = ['StateHoliday']
for column_name in df_1.column_names:
    column_data = df_1[column_name]
    if column_name in my_columns:
        column_data = pa.array(df_1['StateHoliday'].to_pandas().astype('category').cat.codes)
    columns.append(column_data)

updated_table = pa.Table.from_arrays(
    columns, 
    schema=my_schema
)

# COMMAND ----------



# COMMAND ----------

updated_table['StateHoliday'].unique()

# COMMAND ----------

updated_table

# COMMAND ----------

df_1['Date'] =  pd.to_datetime(df_1['Date'])

# COMMAND ----------

df_1.dtypes

# COMMAND ----------

months = [g for n, g in df_1.groupby(pd.Grouper(key='Date',freq='M'))]

# COMMAND ----------

months

# COMMAND ----------

path_to_disk = "/dbfs/mnt/rossmann/partitioned/rossmann-pmonth-000"

# COMMAND ----------

index=0
for name, group in df_1:
  pq.write_table(group, './tmp/pyarrow_out/people1.parquet')
    group.to_parquet(path_to_disk+str(index)+".parquet")
    index+=1

# COMMAND ----------

cols = ["StateHoliday"]
data["StateHoliday"] = data[cols].apply(pd.to_numeric, errors='coerce', axis=1)

# COMMAND ----------

from datetime import datetime

def cur_date():
    return datetime.now().strftime("%Y-%m-%d")

for group in months:
  pq.write_table('{}_{}.parquet'.format(name, cur_date()))
    #group.to_parquet('{}_{}.parquet'.format(name, cur_date()))

# COMMAND ----------

df_1.columns

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
  .master("local") \
  .appName("parquet_example") \
  .getOrCreate()

df = spark.read.csv('/mnt/rossmann/train.csv', header = True, schema="Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long")
df.repartition(30).write.mode('overwrite').parquet('/mnt/rossmann/parquet/')

# COMMAND ----------

# MAGIC %fs ls /mnt/rossmann/tmp/pyspark_rossmann

# COMMAND ----------

mount_name

# COMMAND ----------

dbutils.fs.rm(mount_name+"/tmp", True)

# COMMAND ----------

df.repartition(30).write.mode('overwrite').parquet('/mnt/rossmann/parquet/')

# COMMAND ----------

display(spark.read.format("parquet").schema("Store long, DayOfWeek long, email string, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long").load("/mnt/rossmann/parquet/"))

# COMMAND ----------


