# Azure Databricks read/write Azure SQL Data Warehouse
This is an example to read Azure Open Datasets using Azure Databricks and load a table in Azure SQL Data Warehouse.

Assumption:
* You have access to Azure Databricks
* You have access to Azure SQL Data Warehouse
* Master key has been setup for Azure SQL Data Warehouse. If not then using Admin account run first statement to get a unique string that can be used as encryption key in the next statement
```SQL
select LEFT(REPLACE(CAST(NEWID () AS NVARCHAR(MAX)), '-', '#'), 30) AS ENCKEY;
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<result-of-above>'; 
```
* You have access to Azure Blob Storage

***

STEPS:

 * Start a standard databricks cluster with runtime version 5.3 or above.
 * Create a Scala Notebook
 * Setup connection for Azure Open Datasets - New York City & Boston Safety Data `
```scala
//
val aod_blob_account_name = "azureopendatastorage"
val aod_blob_container_name = "citydatacontainer"
val aod_blob_sas_token = ""
val aod_blob_sas_def = "fs.azure.sas." + aod_blob_container_name + aod_blob_account_name + ".blob.core.windows.net"
//
spark.conf.set(aod_blob_sas_def, aod_blob_sas_token)
//
val aod_blob_NewYorkCity_path = "Safety/Release/city=NewYorkCity"
val aod_blob_Boston_path = "Safety/Release/city=Boston"
//
val wasbs_NewYorkCity_path = "wasbs://" + aod_blob_container_name + "@" + aod_blob_account_name + ".blob.core.windows.net/" + aod_blob_NewYorkCity_path
val wasbs_Boston_path = "wasbs://" + aod_blob_container_name + "@" + aod_blob_account_name + ".blob.core.windows.net/" + aod_blob_Boston_path
```

 * Read and verify sample data
 ```scala
 val NewYorkCity_opendf = spark.read.parquet(wasbs_NewYorkCity_path)
val Boston_opendf = spark.read.parquet(wasbs_Boston_path)
NewYorkCity_opendf.show(5)
Boston_opendf.show(5)
```
* Setup access to your blob storage for Temp (and optional Data) location
```scala
// Set up the Blob Storage account access key in the notebook session conf.
val blobstorage_account = "mystorage106"
val blobStorage = blobstorage_account + ".blob.core.windows.net"
val blobStoragedef = "fs.azure.account.key." + blobStorage
val blobContainer = "mysampledata"
val blobAccessKey =  "myloo..oongaccesskey=="
val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"
spark.conf.set(blobStoragedef, blobAccessKey)
```
* Setup access to your SQL Data Warehouse
```scala
//SQL Data Warehouse related settings
val SQLDWformat = "com.databricks.spark.sqldw"
val dwDatabase = "mysqldw"
val dwServer = "mysqldw-sqlserver.database.windows.net"
val dwUser = "mysqladmin"
val dwPass = "myp@ssword"
val dwJdbcPort =  "1433"
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass
```
* Load data to a table
```scala
//Load to a SQL Data Warehouse table
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

NewYorkCity_opendf
.write
.format(SQLDWformat)
.option("url", sqlDwUrlSmall)
.option("tempdir", tempDir)
.option("forwardSparkAzureStorageCredentials", "true")
.option("dbtable", "dbo.aod_safety_data")       
.save()

//.option( "forward_spark_azure_storage_credentials","True")
```
* Now read from that table - read as a query
```SQL
//Example Load data from a SQL DW query.
val my_query_df = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", sqlDwUrlSmall)
  .option("tempDir", tempDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("query", "select top 10 * from dbo.aod_safety_data")
  .load()
  ```
  * Now read from that table - read entire table
  ```SQL
  //Example Load data from a SQL DW TABLE.
val my_sqldw_table_df = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", sqlDwUrlSmall)
  .option("tempDir", tempDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.aod_safety_data")
  .load()
  ```
* (optional) Load both dataframes (newyorkcity dataset and boston dataset) to a table
```scala
val datasets_opendf = 
NewYorkCity_opendf.withColumn("City",   lit("NewYorkCity"))
.union(Boston_opendf.withColumn("City", lit("Boston     ")))

datasets_opendf
.write
.format(SQLDWformat)
.option("url", sqlDwUrlSmall)
.option("tempdir", tempDir)
.option("forwardSparkAzureStorageCredentials", "true")
.option("dbtable", "dbo.aod_safety_2cities")       
.save()

//.select("city")
//.distinct()
//.show(10)
```
* Done.
