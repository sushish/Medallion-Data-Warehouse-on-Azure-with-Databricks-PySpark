# Databricks notebook source
# MAGIC %md
# MAGIC ### DYNAMIC CAPABILITIES

# COMMAND ----------

dbutils.widgets.text("folder_name","")

# COMMAND ----------

p_folder_name=dbutils.widgets.get("folder_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ***Data Reading***

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation",f"abfss://bronze@databrickstorageaccount1.dfs.core.windows.net/checkpoint_{p_folder_name}")\
        .load(f"abfss://source@databrickstorageaccount1.dfs.core.windows.net/{p_folder_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING 

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation",f"abfss://bronze@databrickstorageaccount1.dfs.core.windows.net/checkpoint_{p_folder_name}")\
    .option("path",f"abfss://bronze@databrickstorageaccount1.dfs.core.windows.net/{p_folder_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

