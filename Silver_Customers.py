# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df=spark.read.format("parquet")\
    .load("abfss://bronze@databrickstorageaccount1.dfs.core.windows.net/customers")
display(df)

# COMMAND ----------

df=df.drop('_rescued_data')
display(df)

# COMMAND ----------

df=df.withColumn("domains",split(col("email"),"@")[1])
display(df)

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).orderBy(desc("total_customers")).display()

# COMMAND ----------

df_gmail=df.filter(col("domains")=="gmail.com")
display(df_gmail)
df_gmail.groupBy("state").agg(count("customer_id").alias("total_customers")).orderBy(desc("total_customers")).display()

# COMMAND ----------

df=df.withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name")))
df.drop("first_name","last_name")
display(df)
df.write.mode("overwrite").format("delta").save("abfss://silver@databrickstorageaccount1.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.customers_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databrickstorageaccount1.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.silver.customers_silver