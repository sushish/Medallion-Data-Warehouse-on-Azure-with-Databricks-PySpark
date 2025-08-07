# Databricks notebook source
# Databricks Notebook - Gold_Product_SCD2

# --- 1. Import necessary functions ---
from pyspark.sql.functions import current_timestamp, sha2, lit, col
import pyspark.sql.functions as F

# --- 2. Define Unity Catalog Table Names ---
unity_catalog_name = "databricks_catalog"
silver_products_table_name = f"{unity_catalog_name}.silver.products_silver"
gold_product_dim_table_name = f"{unity_catalog_name}.gold.product_dim"

# --- 3. Create the Gold Schema (Database) if it doesn't exist ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {unity_catalog_name}.gold")
print(f"Gold schema '{unity_catalog_name}.gold' created or already exists.")

# --- 4. Read the latest product data from the Silver layer ---
products_silver_df = spark.read.table(silver_products_table_name)

# --- 5. Add a hash column to the Silver data to detect changes ---
# The hash should include all columns that, if changed, would trigger a new version.
# We will use this hash to detect if a record has been updated.
products_silver_with_hash = products_silver_df.withColumn(
    "hash_value",
    sha2(
        F.concat(
            F.col("product_name"), 
            F.col("category"), 
            F.col("price"), 
            F.col("supplier")
        ), 
        256
    )
)

# --- 6. Create or Replace a temporary view for the Silver data ---
products_silver_with_hash.createOrReplaceTempView("products_silver_updates")


# --- 7. Check if the Gold table exists before merging ---
table_exists = spark.catalog.tableExists(gold_product_dim_table_name)

if not table_exists:
    print(f"Table '{gold_product_dim_table_name}' does not exist. Creating the table with initial data...")
    
    # For the first run, we'll write the initial data and add the SCD Type 2 columns.
    # The 'product_skey' is the surrogate key and should be a unique ID.
    # The 'effective_end_date' is null for the current version.
    initial_product_dim_df = products_silver_with_hash.withColumn("product_skey", F.monotonically_increasing_id()) \
                                                      .withColumn("effective_start_date", current_timestamp()) \
                                                      .withColumn("effective_end_date", F.lit(None).cast("timestamp")) \
                                                      .withColumn("is_current", F.lit(True)) \
                                                      .withColumn("insert_date", current_timestamp()) \
                                                      .withColumn("update_date", current_timestamp())
    
    initial_product_dim_df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(gold_product_dim_table_name)
else:
    print(f"Table '{gold_product_dim_table_name}' exists. Performing SCD Type 2 MERGE INTO...")
    
    # --- The SCD Type 2 MERGE statement ---
    # This MERGE statement is designed to handle three scenarios in a single transaction:
    # 1. Update (expire) existing records in the dimension table that have changed.
    # 2. Insert new records (the new version of a changed record, or a brand new product).
    # 3. Do nothing for unchanged records.

    sql_merge_statement = f"""
      MERGE INTO {gold_product_dim_table_name} AS target
      USING (
        SELECT 
          *, 
          ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY effective_start_date DESC) as rn
        FROM products_silver_updates
      ) AS source
      ON target.product_id = source.product_id
        AND target.is_current = true
      
      -- Scenario 1: A record in the source has a different hash value, meaning it has changed.
      -- We expire the old record in the target table.
      WHEN MATCHED AND target.hash_value != source.hash_value THEN
        UPDATE SET
          target.is_current = false,
          target.effective_end_date = current_timestamp(),
          target.update_date = current_timestamp()
      
      -- Scenario 2: The source record is either completely new, OR it is a new version
      -- of an existing record that we just expired. We insert this new version.
      WHEN NOT MATCHED THEN
        INSERT (product_skey, product_id, product_name, category, price, supplier, hash_value, effective_start_date, effective_end_date, is_current, insert_date, update_date)
        VALUES (source.product_skey, source.product_id, source.product_name, source.category, source.price, source.supplier, source.hash_value, current_timestamp(), NULL, true, current_timestamp(), current_timestamp())
    """
    
    spark.sql(sql_merge_statement)
    
# --- 8. (Optional) Verify the Gold table contents and metadata ---
print("\nVerifying the contents of the Gold product_dim table...")
gold_product_dim_df = spark.sql(f"SELECT * FROM {gold_product_dim_table_name} ORDER BY product_id, effective_start_date")
gold_product_dim_df.show(truncate=False)

print("\nDescribing the Gold product_dim table...")
spark.sql(f"DESCRIBE DETAIL {gold_product_dim_table_name}").show(truncate=False)
