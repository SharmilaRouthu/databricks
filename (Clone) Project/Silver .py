# Databricks notebook source
# MAGIC %sql
# MAGIC use project;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC df=spark.read.table("project.bronze")
# MAGIC df_final=df.withColumn("products",explode("products"))\
# MAGIC .withColumn("price",col("products.price"))\
# MAGIC .withColumn("product_id",col("products.product_id"))\
# MAGIC .withColumn("product_name",col("products.product_name"))\
# MAGIC .withColumn("quantity",col("products.quantity"))\
# MAGIC .withColumn("timestamp",col("timestamp").cast("timestamp"))\
# MAGIC .drop("products")
# MAGIC df_final.write.mode("overwrite").saveAsTable("project.silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project.bronze

# COMMAND ----------


