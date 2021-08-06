# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Rossmann Data - Step 1
# MAGIC 
# MAGIC  
# MAGIC <img src="https://github.com/tsennikova/databricks-demo/blob/main/Rossmann%20Sales.png?raw=true" width = "800" />
# MAGIC 
# MAGIC <p><b>Usecase:</b></p>
# MAGIC 
# MAGIC <p>Rossmann operates over 3,000 drug stores in 7 European countries. Currently, Rossmann store managers are tasked with predicting their daily sales. Store sales are influenced by many factors, including promotions, competition, school and state holidays, seasonality, and locality. With thousands of individual managers predicting sales based on their unique circumstances, the accuracy of results can be quite varied.</p>
# MAGIC 
# MAGIC <b>In this Notebook we just go through the first step of the data processing. We are reading the raw data and forming our bronze tier.</b><p>
# MAGIC 
# MAGIC *Data Source Acknowledgement: This Data Source Provided By Kaggle*
# MAGIC 
# MAGIC *https://www.kaggle.com/c/rossmann-store-sales/data*

# COMMAND ----------

# DBTITLE 1,Cross Notebook Reference
# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingest

# COMMAND ----------

display(spark.read.format("parquet").schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long").load("/mnt/rossmann/parquet/"))

# COMMAND ----------

# DBTITLE 1,Open Stream for Transactions Table
bronzeDF = spark.readStream\
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long")\
                .load("/mnt/rossmann/parquet/")

# COMMAND ----------

# DBTITLE 1,Read States Mapping Table as Batch
statesDF = spark.read.csv('/mnt/rossmann/store_states_ll.csv', header = True, schema="Store long, State string, Latitude float, Longitude float")
display(statesDF)

# COMMAND ----------

# DBTITLE 1,Merge Batch and Stream
bronzeDF = bronzeDF.join(statesDF, "Store")
display(bronzeDF)

# COMMAND ----------

# DBTITLE 1,Read Store Table as Batch
storeDF = spark.read.csv('/mnt/rossmann/store.csv', 
                          header = True, 
                          schema="Store long, StoreType string, Assortment string, CompetitionDistance long, CompetitionOpenSinceMonth long, CompetitionOpenSinceYear long, Promo2 long, Promo2SinceWeek long, Promo2SinceYear long, PromoInterval string")

# COMMAND ----------

display(storeDF)

# COMMAND ----------

# DBTITLE 1,Enrich Transaction Data with Information about Stores
bronzeDF = bronzeDF.join(storeDF, "Store")
display(bronzeDF)

# COMMAND ----------

dbutils.fs.rm(path+"/retail/_checkpoint", True)

# COMMAND ----------

# DBTITLE 1,We need to keep the cdc. Let's put the data in a Delta table:
bronzeDF.writeStream \
        .trigger(once=True) \
        .option("checkpointLocation", path+"/retail/_checkpoint")\
        .option("mergeSchema", "true")\
        .format("delta") \
        .table("tania.bronze_rossmann_sales")

# optionally:
# .trigger(once=True) \

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tania.bronze_rossmann_sales

# COMMAND ----------

# DBTITLE 1,We can now create our client table using standard SQL command
# MAGIC %sql 
# MAGIC -- we can add NOT NULL in our ID field (or even more advanced constraint)
# MAGIC CREATE TABLE IF NOT EXISTS tania.silver_rossmann_sales (Store INT NOT NULL, DayOfWeek INT, Sales INT, Customers INT, Open INT, Promo INT, StateHoliday STRING, SchoolHoliday INT,Date TIMESTAMP, State STRING, Latitude FLOAT, Longitude FLOAT, StoreType INT, Assortment STRING, CompetitionDistance INT, CompetitionOpenSinceMonth INT, CompetitionOpenSinceYear INT, Promo2 INT, Promo2SinceWeek INT, Promo2SinceYear INT, PromoInterval STRING) USING delta TBLPROPERTIES (delta.enableChangeDataCapture = true) LOCATION '/mnt/rossmann/silver';

# COMMAND ----------

# MAGIC %md ###MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE parquet: multi-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# DBTITLE 1,And run our MERGE statement the upsert the CDC information in our final table
def merge_stream(df, i):
  df.createOrReplaceTempView("rossmann_cdc_microbatch")
  #First we need to dedup the incoming data based on ID (we can have multiple update of the same row in our incoming data)
  #Then we run the merge (upsert or delete). We could do it with a window and filter on rank() == 1 too  
  df._jdf.sparkSession().sql("""MERGE INTO  tania.silver_rossmann_sales target
                                USING
                                (select Store, DayOfWeek, Date, Sales, Customers, Open, Promo, StateHoliday, SchoolHoliday, State, 
                                Latitude, Longitude, StoreType, Assortment, CompetitionDistance, CompetitionOpenSinceMonth,
                                CompetitionOpenSinceYear, Promo2, Promo2SinceWeek, Promo2SinceYear, PromoInterval from 
                                (SELECT *, RANK() OVER (PARTITION BY Store ORDER BY Date DESC) as rank from rossmann_cdc_microbatch) 
                                 where rank = 1
                                ) as source
                                ON source.Store = target.Store
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *  
                                """)  
  
spark.readStream \
       .table("tania.bronze_rossmann_sales") \
       .writeStream.trigger(once=True) \
       .foreachBatch(merge_stream) \
     .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tania.silver_rossmann_sales

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC woow
