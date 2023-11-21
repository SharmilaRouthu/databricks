# Databricks notebook source
inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"
outputpath="dbfs:/mnt/asadlsad/processeddata/outputautoloader"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/sharmila/schemalocation")
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/sharmila/checkpoint")
.option("path",f"{outputpath}/naval/files")
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table stream.firstauto

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/sharmila/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/sharmila/checkpoint")
.option("path",f"{outputpath}/naval/files")
.option("mergeSchema",True)
.table("stream.firstauto")
)

# COMMAND ----------


