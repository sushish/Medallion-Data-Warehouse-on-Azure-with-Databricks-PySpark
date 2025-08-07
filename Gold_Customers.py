# Databricks notebook source
# MAGIC %sql
# MAGIC select * from
# MAGIC (
# MAGIC select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) as row_no from databricks_catalog.silver.customers_silver
# MAGIC )
# MAGIC where row_no > 1;

# COMMAND ----------

# Databricks Notebook - Gold_Customer

# --- 1. Import necessary functions ---
from pyspark.sql.functions import current_timestamp, sha2, monotonically_increasing_id
import pyspark.sql.functions as F

# --- 2. Define Unity Catalog Table Names ---
unity_catalog_name = "databricks_catalog"
silver_customer_table_name = f"{unity_catalog_name}.silver.customers_silver"
gold_customer_table_name = f"{unity_catalog_name}.gold.customer_dim"

# --- 3. Create the Gold Schema (Database) if it doesn't exist ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {unity_catalog_name}.gold")
print(f"Gold schema '{unity_catalog_name}.gold' created or already exists.")

# --- 4. Read the latest customer data from the Silver layer ---
silver_customers_df = spark.read.table(silver_customer_table_name)

# --- 5. Add the Surrogate Key and Hash for change detection ---
# We use monotonically_increasing_id() to generate a unique,
# ever-increasing integer ID for new records. This is the best practice
# for creating surrogate keys in a distributed environment like Databricks.
silver_customers_with_keys = silver_customers_df.withColumn(
    "customer_skey", monotonically_increasing_id()
).withColumn(
    "hash_value",
    sha2(
        F.concat(
            F.col("first_name"), 
            F.col("last_name"), 
            F.col("email"), 
            F.col("city"), 
            F.col("state"),
            F.col("fullname"),
            F.col("domains")
        ), 
        256
    )
)

# --- 6. Create or Replace a temporary view for the Silver data ---
silver_customers_with_keys.createOrReplaceTempView("silver_customers_updates")

# --- 7. Check if the Gold table exists before merging ---
table_exists = spark.catalog.tableExists(gold_customer_table_name)

if not table_exists:
    print(f"Table '{gold_customer_table_name}' does not exist. Creating the table with initial data...")
    
    # For the first run, we'll write the initial data and add the audit columns.
    silver_customers_final = silver_customers_with_keys.withColumn("created_date", current_timestamp()) \
                                                       .withColumn("updated_date", current_timestamp()) \
                                                       .withColumn("change_type", F.lit("I"))
    
    silver_customers_final.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(gold_customer_table_name)
else:
    print(f"Table '{gold_customer_table_name}' exists. Performing MERGE INTO...")
    
    # --- Define the SQL MERGE statement for SCD Type 1 logic with audit columns ---
    # The MERGE condition now includes the composite primary key.
    # The customer_skey is ONLY inserted for new records and is not updated.
    sql_merge_statement = f"""
      MERGE INTO {gold_customer_table_name} AS target
      USING silver_customers_updates AS source
      ON target.customer_id = source.customer_id
        AND target.first_name = source.first_name
        AND target.last_name = source.last_name
      WHEN MATCHED AND (target.hash_value != source.hash_value) THEN
        UPDATE SET
          target.first_name = source.first_name,
          target.last_name = source.last_name,
          target.email = source.email,
          target.city = source.city,
          target.state = source.state,
          target.fullname = source.fullname,
          target.domains = source.domains,
          target.updated_date = current_timestamp(),
          target.change_type = 'U',
          target.hash_value = source.hash_value
      WHEN NOT MATCHED THEN
        INSERT (customer_skey, customer_id, first_name, last_name, email, city, state, fullname, domains, created_date, updated_date, change_type, hash_value)
        VALUES (source.customer_skey, source.customer_id, source.first_name, source.last_name, source.email, source.city, source.state, source.fullname, source.domains, current_timestamp(), current_timestamp(), 'I', source.hash_value)
    """
    spark.sql(sql_merge_statement)

# --- 8. (Optional) Verify the Gold table contents and metadata ---
print("\nVerifying the contents of the Gold table...")
gold_customer_dim_df = spark.sql(f"SELECT * FROM {gold_customer_table_name}")
gold_customer_dim_df.show()

print("\nDescribing the Gold table...")
spark.sql(f"DESCRIBE DETAIL {gold_customer_table_name}").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catalog.gold.customer_dim;