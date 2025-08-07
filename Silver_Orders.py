# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Read

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.format("parquet")\
    .load("abfss://bronze@databrickstorageaccount1.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.withColumnRenamed("_rescued_data","rescued_data").display()

# COMMAND ----------

df=df.drop("_rescued_data")

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("order_date",to_timestamp(col('order_date')))
display(df)

# COMMAND ----------

df=df.withColumn("year",year(col('order_date')))
display(df)

# COMMAND ----------

df1=df.withColumn("dense_flag",dense_rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
display(df1)

# COMMAND ----------



# COMMAND ----------

df1=df1.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
display(df1)

# COMMAND ----------

df1=df1.withColumn("row_number_flag",row_number().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
display(df1)

# COMMAND ----------

display(df)

# COMMAND ----------

class windows:
  def dense_rank(self,df):
    df_dense_rank=df.withColumn("dense_flag",dense_rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
    return df_dense_rank
  def rank(self,df):
    df_rank=df.withColumn("rank_flag",rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
    return df_rank
  def row_number(self,df):
    df_row_number=df.withColumn("row_number_flag",row_number().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
    return df_row_number
w=windows()
df_dense_rank=w.dense_rank(df)
display(df_dense_rank)
df_rank=w.rank(df)
display(df_rank)
df_row_number=w.row_number(df)
display(df_row_number)

# COMMAND ----------

w=windows()
df_dense_rank=w.dense_rank(df)
df_rank=w.rank(df_dense_rank)
df_row_number=w.row_number(df_rank)
df_new=df_row_number
display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickstorageaccount1.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_catalog.silver.orders_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databrickstorageaccount1.dfs.core.windows.net/orders'