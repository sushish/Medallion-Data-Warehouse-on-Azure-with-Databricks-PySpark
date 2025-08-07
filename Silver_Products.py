# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df=spark.read.format("Parquet")\
    .load("abfss://bronze@databrickstorageaccount1.dfs.core.windows.net/products")
df=df.drop("_rescued_data")
display(df)


# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_catalog.bronze.discount(p_price double) returns double
# MAGIC language sql
# MAGIC return p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,databricks_catalog.bronze.discount(price) as discounted_price from products

# COMMAND ----------

df=df.withColumn("discounted_price",expr("databricks_catalog.bronze.discount(price)"))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function  databricks_catalog.bronze.upper_func(p_brand string) returns string
# MAGIC language python 
# MAGIC as
# MAGIC $$
# MAGIC return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, databricks_catalog.bronze.upper_func(brand) as brand_upper from products

# COMMAND ----------

display(df)

df.write.format("delta") \
  .mode("overwrite") \
  .save("abfss://silver@databrickstorageaccount1.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table databricks_catalog.silver.pruducts_silver;
# MAGIC
# MAGIC create table if not exists databricks_catalog.silver.products_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@databrickstorageaccount1.dfs.core.windows.net/products'