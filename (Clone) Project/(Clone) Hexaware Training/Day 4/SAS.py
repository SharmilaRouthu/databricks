# Databricks notebook source
# MAGIC %fs ls 
# MAGIC dbfs:/mnt/asadlsad/processeddata/
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/iotdata/

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sradlshexaware.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sradlshexaware.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sradlshexaware.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=c&sp=rlx&se=2023-11-14T02:03:00Z&st=2023-11-13T18:03:00Z&spr=https&sig=ornBaEJR%2FPbqvdrN%2BLGQVQjZ63DvVFCNi1jQRuhPS7o%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@sradlshexaware.dfs.core.windows.net/fact/")

# COMMAND ----------

dbutils.fs.unmount("/mnt/asadlsad/processeddata")

# COMMAND ----------

# MAGIC %fs ls /mnt/asadlsad/

# COMMAND ----------


