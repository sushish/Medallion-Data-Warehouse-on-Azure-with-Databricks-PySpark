# Databricks notebook source
# MAGIC %md
# MAGIC ### DELTA LIVE TABLE
# MAGIC
# MAGIC This DLT script achieves the same result as the previous PySpark notebook but with a much more streamlined approach.
# MAGIC
# MAGIC Key differences and what you need to do:
# MAGIC Declarative Syntax: Instead of writing a complex MERGE INTO statement, you simply declare the desired outcome using dlt.apply_changes. DLT handles all the underlying logic for you, including expiring old records and inserting new ones.
# MAGIC
# MAGIC Streaming: The dlt.read_stream() function automatically processes new data as it arrives in the Silver table, making this an incremental and efficient pipeline.
# MAGIC
# MAGIC Deployment: To use this script, you would deploy it as a new Delta Live Tables pipeline in Databricks. You don't "run" the notebook like a standard PySpark script; instead, you define the pipeline in the DLT UI and let the framework manage the execution.
# MAGIC
# MAGIC Transient Silver Layer: Your existing practice of overwriting the Silver layer is perfectly compatible with DLT. When new data overwrites the Silver table, the DLT pipeline will automatically detect the changes and apply them to the Gold product_dim table on its next scheduled update.
# MAGIC
# MAGIC Delta Live Tables (DLT) is a framework built on Apache Spark and Delta Lake that simplifies the development and management of data pipelines. It's a declarative, unified, and opinionated approach to building robust ETL (Extract, Transform, Load) processes in Databricks.
# MAGIC
# MAGIC Here is a breakdown of the key knowledge points about DLT:
# MAGIC
# MAGIC 1. The Core Idea: Declarative vs. Imperative
# MAGIC Traditional ETL (Imperative): With a standard PySpark notebook, you write step-by-step instructions. You tell the system how to do things: "read this file, join it with that table, then write the result, and finally run a MERGE INTO statement to update the destination." This can be complex and error-prone.
# MAGIC
# MAGIC DLT (Declarative): With DLT, you simply declare the desired state of your tables. You define the relationships between your tables and the transformations that should be applied. DLT's engine then figures out the most efficient way to achieve that state, handling all the complex orchestration and dependencies for you.
# MAGIC
# MAGIC
# MAGIC 2. Key Principles and Features
# MAGIC a) Live Tables and Views
# MAGIC DLT pipelines are composed of "live tables" and "live views." These are not static tables; they are continuously updated based on the data flowing through your pipeline.
# MAGIC
# MAGIC @dlt.table: A live table is materialized and stored as a Delta table. It can be used as a source for other tables.
# MAGIC
# MAGIC @dlt.view: A live view is not materialized; it's a temporary view that is computed on-the-fly when referenced. This is useful for intermediate transformations that don't need to be stored.
# MAGIC
# MAGIC b) Automatic Dependency Management
# MAGIC When you define a DLT pipeline, the framework automatically creates a data lineage graph. It understands the dependencies between your tables and views, so it knows the correct order of operations. You don't need to manually orchestrate jobs or manage dependencies yourself.
# MAGIC
# MAGIC
# MAGIC For example, if gold_table depends on silver_table, DLT will ensure silver_table is updated before it attempts to update gold_table.
# MAGIC
# MAGIC c) Built-in Data Quality with Expectations
# MAGIC DLT introduces the concept of "expectations" to define data quality rules directly in your code. You can specify what you expect from your data (e.g., "the price column should never be null") and DLT will automatically enforce these rules.
# MAGIC
# MAGIC
# MAGIC You define expectations using decorators like @dlt.expect, @dlt.expect_or_drop, and @dlt.expect_or_fail. DLT can then:
# MAGIC
# MAGIC Report on failed records.
# MAGIC
# MAGIC Drop records that don't meet the expectation.
# MAGIC
# MAGIC Fail the entire pipeline if a critical expectation is violated.
# MAGIC
# MAGIC This makes it easy to build reliable pipelines and monitor data quality from a single place.
# MAGIC
# MAGIC d) Automated SCD Type 2 Logic
# MAGIC As you saw in the previous example, DLT has built-in support for Slowly Changing Dimensions. The dlt.apply_changes() function simplifies the complex logic of expiring old records and inserting new ones. You just tell DLT which keys to track and which columns to monitor for changes, and it handles the rest.
# MAGIC
# MAGIC 3. Benefits of Using DLT
# MAGIC Simplified Code: DLT's declarative syntax significantly reduces the amount of code you need to write and maintain, especially for complex transformations and change data capture.
# MAGIC
# MAGIC Automated Management: DLT manages the underlying infrastructure (clusters), job scheduling, and dependencies. This frees up data engineers to focus on business logic rather than operational overhead.
# MAGIC
# MAGIC Reliability: With built-in expectations, DLT makes it easy to build robust pipelines that are resilient to data quality issues. Failed records can be isolated and analyzed without corrupting the entire table.
# MAGIC
# MAGIC Unified Batch and Streaming: DLT pipelines can seamlessly handle both batch and streaming data. By simply changing a function from dlt.read() to dlt.read_stream(), you can turn a batch process into a streaming one.
# MAGIC
# MAGIC Observability: The DLT UI provides a visual representation of your pipeline's lineage, data flow, and health metrics, making it easy to monitor and troubleshoot your pipelines.

# COMMAND ----------

#Expectations
product_rule ={
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "product_name IS NOT NULL"
}

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

source_products_table="databricks_catalog.silver.products_silver"
dlt.expect_all_or_drop(product_rule) # expectations on multiple rules

@dlt.table(
  comment="SCD Type 2 dimension table for products, tracking historical changes."
)
def DimProducts_stage():
  df = spark.readStream.table(source_products_table)
  return df

@dlt.view()
def DimProducts_view():
  df = spark.readStream.table("Live.DimProducts_stage")
  return df

dlt.create_streaming_table("product_dim")

dlt.apply_changes(
        target="product_dim",
        source="Live.DimProducts_view",
        keys=["product_id"],
        sequence_by=current_timestamp(),  # current_timestamp() Ideally, replace with a real timestamp column
        stored_as_scd_type=2,
        track_history_column_list=["product_name", "category", "price", "brand"]
    )