# Databricks notebook source
df=spark.sql("select * from databricks_catalog.silver.orders_silver")
display(df)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
import pyspark.sql.functions as F

# The fact table you loaded in DLT is a great starting point,
# but this is how you would perform a similar operation in a standard
# PySpark notebook as a batch job.

# --- 1. DEFINE SOURCE AND TARGET TABLES ---
silver_orders_table = "databricks_catalog.silver.orders_silver"
gold_customer_dim = "databricks_catalog.gold.customer_dim"
gold_product_dim = "databricks_catalog.gold.product_dim"
gold_order_fact = "databricks_catalog.gold.order_fact"
fact_table_path = "abfss://gold@databrickstorageaccount1.dfs.core.windows.net/gold/order_fact"


# --- 2. READ THE INCOMING INCREMENTAL DATA FROM THE SILVER LAYER ---
# This is the crucial step that was missing. You must read the data
# you want to process from your silver layer table.
# In a real-world scenario, you would have a way to filter this to only
# new or updated records since the last run. For this example, we read all data.
incremental_df = spark.read.format("delta").table(silver_orders_table)

# --- 3. ENRICH THE DATA WITH SURROGATE KEYS FROM DIMENSIONS ---
# Read the dimension tables to join with the incremental data.
df_cust = spark.sql(f"select customer_skey, customer_id from {gold_customer_dim}")
df_prod = spark.sql(f"select product_id as product_skey, product_id from {gold_product_dim}")

# Join the incremental data with the dimension tables.
df_fact = incremental_df.join(df_cust, incremental_df['customer_id'] == df_cust['customer_id'], how='left')\
                        .join(df_prod, incremental_df['product_id'] == df_prod['product_id'], how='left')

# --- 4. PREPARE THE FINAL SCHEMA ---
# Select the final columns and ensure they are in the correct order.
# The `created_dt` and `updated_dt` columns are added here.
df_fact = df_fact.select(
    'order_id',
    'order_date',
    'year',
    'customer_skey',
    'product_skey',
    'quantity',
    'total_amount',
    # Add new timestamp columns
    F.lit(current_timestamp()).alias("created_dt"),
    F.lit(current_timestamp()).alias("updated_dt")
)

# --- 5. PERFORM MERGE OR INITIAL WRITE ---
# This is the logic you provided, which correctly handles the initial load
# and subsequent incremental merges.
if spark.catalog.tableExists(gold_order_fact):
    # MERGE: If the fact table exists, perform an upsert.
    delta_table_obj = DeltaTable.forName(spark, gold_order_fact)
    delta_table_obj.alias("t").merge(
        df_fact.alias("s"),
        "t.order_id=s.order_id"
    )\
    .whenMatchedUpdate(set = {
        # Only update the updated_dt column for matched records
        "updated_dt": F.current_timestamp()
    })\
    .whenNotMatchedInsert(values = {
        # Insert all columns for new records
        "order_id": F.col("s.order_id"),
        "order_date": F.col("s.order_date"),
        "year": F.col("s.year"),
        "customer_skey": F.col("s.customer_skey"),
        "product_skey": F.col("s.product_skey"),
        "quantity": F.col("s.quantity"),
        "total_amount": F.col("s.total_amount"),
        "created_dt": F.current_timestamp(),
        "updated_dt": F.current_timestamp()
    })\
    .execute()
else:
    # INITIAL LOAD: If the table does not exist, create it for the first time.
    df_fact.write.format("delta")\
        .option("path", fact_table_path)\
        .saveAsTable(gold_order_fact)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.order_fact;