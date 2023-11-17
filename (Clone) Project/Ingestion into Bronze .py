# Databricks notebook source
# MAGIC %sql
# MAGIC create schema  if not exists project;

# COMMAND ----------

# MAGIC %sql
# MAGIC use project;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze
# MAGIC FROM 'dbfs:/mnt/asadlsad/processeddata/inputproject/json'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS('multiline'='True')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze

# COMMAND ----------


