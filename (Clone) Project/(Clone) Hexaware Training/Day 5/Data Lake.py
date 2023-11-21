# Databricks notebook source
https://docs.delta.io/latest/delta-batch.html#create-a-table

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema srdelta;
# MAGIC use srdelta

# COMMAND ----------

# MAGIC %sql
# MAGIC use srdelta;
# MAGIC CREATE TABLE IF NOT EXISTS srdelta.people10m (  id INT,  firstName STRING,  middleName STRING,  lastName STRING,  gender STRING,  birthDate TIMESTAMP,  ssn STRING,  salary INT) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended srdelta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history srdelta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS srdelta.people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) LOCATION 'dbfs:/mnt/sradlshexaware/processeddata/delta/sharmila'

# COMMAND ----------


