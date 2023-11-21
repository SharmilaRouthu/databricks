# Databricks notebook source
df = spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_final=df.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name","url")

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_final.write.saveAsTable("formula1.drivers")

# COMMAND ----------


