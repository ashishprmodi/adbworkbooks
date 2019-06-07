// Databricks notebook source
// DBTITLE 1,Initialized configuration settings
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "bdca5d18-5709-4c9c-b40d-bb236de77d3a",
  "fs.azure.account.oauth2.client.secret" -> "tDj:jY8GaJ6wR[7YfkuYxj/V?DwzD0]w",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------


dbutils.fs.mount(
  source = "abfss://data@mstranninggen2.dfs.core.windows.net/",
  mountPoint = "/mnt/dlsdata",
  extraConfigs = configs)


// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/dlsdata

// COMMAND ----------


import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileName = "/mnt/dlsdata/ratings-c.csv"
val schema = StructType(
Array(
StructField("userId",IntegerType,true),
StructField("movieId",IntegerType,true),
StructField("rating",DoubleType,true),
StructField("timestamp",StringType,true)
)
)

// COMMAND ----------

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileName)

// COMMAND ----------

data.printSchema

// COMMAND ----------

data.createOrReplaceTempView("ratings")

// COMMAND ----------

val result = spark.sql("SELECT rating, COUNT(*) as totalratings FROM ratings GROUP BY rating ORDER BY totalratings DESC")
result.createOrReplaceTempView("processedresults")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM processedresults