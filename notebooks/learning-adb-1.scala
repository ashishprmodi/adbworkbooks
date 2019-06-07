// Databricks notebook source
// DBTITLE 1,Accessing Blob Storage and Processing the Data
spark.conf.set("fs.azure.account.key.mstrainingstorage.blob.core.windows.net", "7FKvoK22Xc78JCIoaqvA5zjSkFW4uqTWhxf9vKt2cDTyOKyL9fPzMQiSN4ZAnwAKzIwB5in50N+2dyZZB5NypQ==") 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("wasbs://data@mstrainingstorage.blob.core.windows.net/").foreach(println)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS ratings;
// MAGIC CREATE TABLE ratings
// MAGIC USING CSV
// MAGIC OPTIONS (
// MAGIC  path "wasbs://data@mstrainingstorage.blob.core.windows.net/ratings-c.csv",
// MAGIC  header "true"
// MAGIC );

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from ratings;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT rating, COUNT(*) as TotalRatings
// MAGIC FROM ratings
// MAGIC GROUP BY rating
// MAGIC Order by TotalRatings DESC