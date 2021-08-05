# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Rossmann Stores Sales Prediction
# MAGIC 
# MAGIC In this example, we demonstrate how to forecast sales using store, promotion, and competitor data.
# MAGIC 
# MAGIC <img src="https://github.com/tsennikova/databricks-demo/blob/main/Rossmann%20Sales.png?raw=true" width = "800" />
# MAGIC 
# MAGIC <p><b>Usecase:</b></p>
# MAGIC 
# MAGIC <p>Rossmann operates over 3,000 drug stores in 7 European countries. Currently, Rossmann store managers are tasked with predicting their daily sales. Store sales are influenced by many factors, including promotions, competition, school and state holidays, seasonality, and locality. With thousands of individual managers predicting sales based on their unique circumstances, the accuracy of results can be quite varied.</p>
# MAGIC 
# MAGIC <p><b>Demo Objective:</b></p>
# MAGIC 
# MAGIC <ol>
# MAGIC <li> Show change data capture from streaming source</li>
# MAGIC <li> Show unified batch and streaming</li>
# MAGIC <li> Show data management with Delta (deletes, updates, schema evolution, time trevel)</li>
# MAGIC <li> Show cluster management capability</li>
# MAGIC <li> Show how to track ML model parameters, metrics, tags and artifacts  </li>
# MAGIC <li> Show Databricks platform seamless collaboration capability</li>
# MAGIC <li> Demonstrate the simplicity of managing multiple model runs</li>
# MAGIC <li> Show model deployment and serving via Rest API and batch</li>
# MAGIC <li> Show model versioning and state transition from Staging to Production</li>
# MAGIC <li> Show building dashboards in SQL Analytics</li>
# MAGIC </ol>
# MAGIC 
# MAGIC 
# MAGIC <b>Content</b><p>
# MAGIC The data comtain historical sales for 1,115 Rossmann stores. </br>
# MAGIC <b>Files</b>
# MAGIC <li>transactions.parquet - historical data including Sales</li>
# MAGIC <li>store.csv - supplemental information about the stores</li>
# MAGIC <li>store_states.csv - mapping of German States</li>
# MAGIC 
# MAGIC 
# MAGIC <p>We will use Gradient Boosted Tree Regression to predict Sales</p>
# MAGIC <p>
# MAGIC Once the model is trained, we'll use MFLow to track its performance and save it in the registry to deploy it in production</p>
# MAGIC 
# MAGIC 
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
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("Store long, DayOfWeek long, Date timestamp, Sales long, Customers long, Open long, Promo long, StateHoliday string, SchoolHoliday long") \
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
        .trigger(processingTime='10 seconds') \
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
       .writeStream \
       .foreachBatch(merge_stream) \
       .trigger(processingTime='10 seconds') \
     .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tania.silver_rossmann_sales

# COMMAND ----------

#dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Analysis

# COMMAND ----------

import numpy as np
import databricks.koalas as ks

# data visualization
import matplotlib.pyplot as plt
import seaborn as sns # advanced vizs
%matplotlib inline

# COMMAND ----------

# DBTITLE 1,Data Analysis with Koalas
silver_df = ks.read_delta("/mnt/rossmann/silver")

# COMMAND ----------

# DBTITLE 1,Check for the NULL values
silver_df.isnull().sum()

# COMMAND ----------

# DBTITLE 1,Compute the Correlation Matrix 
numeric_df = silver_df.drop(['StateHoliday', 'Date', 'Assortment', 'Assortment', 'State', 'PromoInterval', 'StoreType'], axis = 1)
numeric_df.fillna(0,inplace=True)
corr_all = numeric_df.corr()

# Set up the matplotlib figure
f, ax = plt.subplots(figsize = (11, 9))

# Draw the heatmap with the mask and correct aspect ratio
sns.heatmap(corr_all.values,square = True, linewidths = .5, ax = ax, cmap = "BuPu", yticklabels=corr_all.columns, xticklabels=corr_all.columns)      
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Delta DML Support
# MAGIC 
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tania.silver_rossmann_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete Support
# MAGIC 
# MAGIC Let's `DELETE` `NULL` values from `CompetitionOpenSinceMonth`

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when CompetitionOpenSinceMonth is null then 1 else 0 end) as CompetitionOpenSinceMonth_NULL from tania.silver_rossmann_sales 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tania.silver_rossmann_sales WHERE CompetitionOpenSinceMonth is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when CompetitionOpenSinceMonth is null then 1 else 0 end) as CompetitionOpenSinceMonth_NULL from tania.silver_rossmann_sales 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Support
# MAGIC Let's `UPDATE` `NULL` values in `Promo2SinceWeek`

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when Promo2SinceWeek is null then 1 else 0 end) as Promo2SinceWeek_NULL from tania.silver_rossmann_sales 

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE tania.silver_rossmann_sales SET Promo2SinceWeek = 0 WHERE Promo2SinceWeek is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when Promo2SinceWeek is null then 1 else 0 end) as Promo2SinceWeek_NULL from tania.silver_rossmann_sales 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tania.silver_rossmann_sales Limit 5

# COMMAND ----------

# DBTITLE 1,Add SalesPerCustomer column, remove StoreType
# Generate new data with wrong ids
sales_per_customer_clean = sql("select Sales / Customers as SalesPerCustomer, Store, DayOfWeek, Date, Sales, Customers, Open, Promo, StateHoliday, SchoolHoliday, State, Latitude, Longitude, Assortment, CompetitionDistance, CompetitionOpenSinceMonth, CompetitionOpenSinceYear, Promo2, Promo2SinceWeek, Promo2SinceYear, PromoInterval from tania.silver_rossmann_sales")
display(sales_per_customer_clean)

# COMMAND ----------

# Let's write this data out to our Delta table
sales_per_customer_clean.write.format("delta").mode("append").save("/mnt/rossmann/silver")

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the schema of our new data does not match the schema of our original data

# COMMAND ----------

# Add the mergeSchema option
sales_per_customer_clean.write.option("mergeSchema","true").format("delta").mode("append").save("/mnt/rossmann/silver")

# COMMAND ----------

# MAGIC %md **Note**: With the `mergeSchema` option, we can merge these different schemas together.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current state within the `tania.silver_rossmann_sales` Delta Lake table
# MAGIC select * from tania.silver_rossmann_sales

# COMMAND ----------

# MAGIC %md ## Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tania.silver_rossmann_sales

# COMMAND ----------

# MAGIC %md ### Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when CompetitionOpenSinceMonth is null then 1 else 0 end) as CompetitionOpenSinceMonth_NULL from tania.silver_rossmann_sales VERSION AS OF 4

# COMMAND ----------

# MAGIC %md ### Vacuum
# MAGIC By default, Delta Lake retains table history for 30 days and makes it available for “time travel” and rollbacks.
# MAGIC If you determine that GDPR or CCPA compliance requires that these stale records be made unavailable for querying before the default retention period is up, you can use the VACUUM function to remove files that are no longer referenced by a Delta table and are older than a specified retention threshold. Once you have removed table history using the VACUUM command, all users lose the ability to view that history and roll back.

# COMMAND ----------

# MAGIC %md
# MAGIC To remove artifacts younger than 7 days, use the RETAIN num HOURS option:

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false") 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM tania.silver_rossmann_sales RETAIN 0.01 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY tania.silver_rossmann_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(case when CompetitionOpenSinceMonth is null then 1 else 0 end) as CompetitionOpenSinceMonth_NULL from tania.silver_rossmann_sales VERSION AS OF 4

# COMMAND ----------

# MAGIC %md ##Machine Learning with Databricks

# COMMAND ----------

import pandas as pd
import xgboost as xgb
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from pyspark.ml import Pipeline
import mlflow.xgboost
from pyspark.ml.feature import StringIndexer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# COMMAND ----------

# MAGIC %md ###Data Preparation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tania.silver_rossmann_ml;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE tania.silver_rossmann_ml
# MAGIC USING Delta
# MAGIC LOCATION "/mnt/rossmann/silver_ml"
# MAGIC AS SELECT Store, DayOfWeek, Sales, Customers, Open, Promo, SchoolHoliday, Latitude, Longitude, CompetitionDistance, CompetitionOpenSinceMonth, CompetitionOpenSinceYear, Promo2, Promo2SinceWeek, Promo2SinceYear 
# MAGIC FROM tania.silver_rossmann_sales VERSION AS OF 6

# COMMAND ----------

# DBTITLE 1,Read Data in the Spark Dataframe
mlDF = spark.read.format("delta").load("/mnt/rossmann/silver_ml").na.fill(0)

# COMMAND ----------

# DBTITLE 1,Split Data into Train/Test sets
train_x, test_x, train_y, test_y = train_test_split(mlDF.select("Store", "DayOfWeek",  "Customers", "Open", "Promo",
                                                                "SchoolHoliday", "Latitude", "Longitude", 
                                                                "CompetitionDistance", "CompetitionOpenSinceMonth",
                                                                "CompetitionOpenSinceYear", "Promo2", "Promo2SinceWeek",
                                                                "Promo2SinceYear").toPandas(),
                                                    mlDF.select("Sales").toPandas().values.ravel(), 
                                                    random_state=42)

train = xgb.DMatrix(data=train_x, label=train_y)
test = xgb.DMatrix(data=test_x, label=test_y)

# COMMAND ----------

def train_model(params):
  with mlflow.start_run(nested=True) as run:
   
    booster = xgb.train(params=params, dtrain=train, num_boost_round=1000,\
                        evals=[(test, "test")], early_stopping_rounds=10, verbose_eval=100)   
    predictions_test = booster.predict(test)
    
    mae = mean_absolute_error(test_y, predictions_test)
    mse = mean_squared_error(test_y, predictions_test)
    r2 = r2_score (test_y, predictions_test)
    
    mlflow.log_metric('mae', mae)
    mlflow.log_metric('mse', mse)
    mlflow.log_metric('r2', r2)
    
    model_uri = f"runs:/{run.info.run_uuid}/model" # model identifier representing this run!
    return { "model_uri":model_uri, 'loss': -1*r2, 'booster': booster.attributes()}

# COMMAND ----------

mlflow.xgboost.autolog()
result = train_model(
  {
    'learning_rate': 0.4,
     'max_depth': 65,
     'min_child_weight': 0.6,
     'reg_alpha': 0.15,
     'reg_lambda': 0.2,
     'objective': 'reg:squarederror'
  }
)
r2 = -1*result['loss']
r2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run predictions on python [Single node]

# COMMAND ----------

def predict_python(model_uri, data):  
  model = mlflow.pyfunc.load_model(model_uri)
  predictions = model.predict(data)
  df = pd.DataFrame(predictions.astype(int),columns=["prediction"])
  return df
preds_df = predict_python(result["model_uri"],test_x)
preds_df.head(5)

# COMMAND ----------

frames = [test_x.reset_index(drop=True), preds_df.reset_index(drop=True), pd.Series(test_y).rename("Real_Sales")]
goldDF = pd.concat(frames, axis=1)
display(goldDF.loc[goldDF['Store'] < 150])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run predictions on spark [Multi node]

# COMMAND ----------

def predict_spark(model_uri, data):  
  input = spark.createDataFrame(data)
  udf = mlflow.pyfunc.spark_udf(spark, model_uri)
  predictions = input.withColumn("predictions", udf(*input.columns)).select("predictions")
  return predictions.select(col("predictions").cast("int"))

display(predict_spark(result["model_uri"],test_x))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the model and use the Rest API to run some predictions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Please follow the recorded snippet bellow to register a model
# MAGIC 
# MAGIC <img src = "https://drive.google.com/uc?export=download&id=1QN2GxApZzfJcLGf_QDAdIWDPcqfVTbKG" width = "600">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Predict from model registry

# COMMAND ----------

model_registry_name = "rossmann_sales" # Write the model name you created here. 

# COMMAND ----------

# DBTITLE 1,Run batch prediction from Staging specific version
model_uri = f"models:/{model_registry_name}/1"
display(predict_python(model_uri,test_x))

# COMMAND ----------

# MAGIC %md
# MAGIC ### After model Serving is loaded. you can start serving.
# MAGIC 
# MAGIC <img src = "https://drive.google.com/uc?export=download&id=1pxhFSfd_yVytBuNrY_u7femPOXuZVj1M" width="600">
# MAGIC 
# MAGIC > Try using this sample : 
# MAGIC 
# MAGIC ``` 
# MAGIC [
# MAGIC   {
# MAGIC     "Store": 598,
# MAGIC     "DayOfWeek": 5,
# MAGIC     "Customers": 600,
# MAGIC     "Promo": 0,
# MAGIC     "Latitude": 48.137154,
# MAGIC     "Longitude": 11.576124,
# MAGIC     "CompetitionDistance": 550,
# MAGIC     "CompetitionOpenSinceMonth": 12,
# MAGIC     "CompetitionOpenSinceYear": 2013,
# MAGIC     "Promo2": 1,
# MAGIC     "Promo2SinceWeek": 40,
# MAGIC     "Promo2SinceYear": 2014,
# MAGIC     "Open": 1,
# MAGIC     "SchoolHoliday": 0
# MAGIC   }
# MAGIC ]```

# COMMAND ----------

spark.createDataFrame(goldDF).write.format("delta").mode("overwrite").saveAsTable("tania.gold_rossmann_sales")
spark.createDataFrame(goldDF).write.format("delta").mode("overwrite").save("/mnt/rossmann/gold")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Our data is ready! Let's create a dashboard to monitor the sales
# MAGIC 
# MAGIC https://eastus2.azuredatabricks.net/sql/dashboards/583a9565-5beb-4e69-a223-aaf9addf9867-rossmann?o=5206439413157315

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Rossmann Stores Sales Prediction
# MAGIC 
# MAGIC <img src="https://github.com/tsennikova/databricks-demo/blob/main/Rossmann%20Sales.png?raw=true" width = "800" />

# COMMAND ----------


