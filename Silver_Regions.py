# Databricks notebook source
df=spark.read.table("databricks_catalog.bronze.regions")
display(df)

# COMMAND ----------

df=df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@databrickstorageaccount1.dfs.core.windows.net/regions")

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@databrickstorageaccount1.dfs.core.windows.net/products")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.regions_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databrickstorageaccount1.dfs.core.windows.net/regions'