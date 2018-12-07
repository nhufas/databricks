// Databricks notebook source
/*devhdi02*/
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "<your-service-client-id>")
spark.conf.set("dfs.adls.oauth2.credential", "<your-service-credentials>")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/<your-directory-id>/oauth2/token")

// COMMAND ----------

val df = spark.read.parquet("adl://<your-data-lake-store-account-name>.azuredatalakestore.net/<your-directory-name>")

dbutils.fs.ls("adl://<your-data-lake-store-account-name>.azuredatalakestore.net/<your-directory-name>")

// COMMAND ----------

/*devhdi01*/
"dfs.adls.oauth2.client.id": "31ed40a6-8963-4119-a532-d7b07784a42f",
"dfs.adls.oauth2.credential": "THqLDOmRTUMQvuPYsD+motcBT4jHb7FdwdkrPCER7Jg=",
"dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/95e66ecc-f2c2-464b-84d9-8fda407bc923/oauth2/token"}





// COMMAND ----------

/*devhdi01*/
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "31ed40a6-8963-4119-a532-d7b07784a42f")
spark.conf.set("dfs.adls.oauth2.credential", "THqLDOmRTUMQvuPYsD+motcBT4jHb7FdwdkrPCER7Jg=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/95e66ecc-f2c2-464b-84d9-8fda407bc923/oauth2/token")






// COMMAND ----------

val dfprd = sqlContext.read.format("csv")
  .option("header", "false")
  .option("inferSchema", "true")
  .option("delimiter","|")
  .load("adl://devrbdls01.azuredatalakestore.net/onedata-platform/landing/TE/execution/RB_EXECUTION_ACTIVITY_INVOICING/region=SOA/")

//adl://devrbdls01.azuredatalakestore.net/onedata-platform/landing/TE/execution/RB_EXECUTION_ACTIVITY_INVOICING/region=SOA/RB_EXECUTION_ACTIVITY_INVOICING_BGD_20181101063000.gz

// COMMAND ----------

dfprd.count()

// COMMAND ----------

display(dbutils.fs.ls("adl://devrbdls01.azuredatalakestore.net/onedata-platform/landing/TE/execution/RB_EXECUTION_ACTIVITY_INVOICING/region=SOA/"))