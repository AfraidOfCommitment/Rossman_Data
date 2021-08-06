# Databricks notebook source
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
