# Databricks notebook source
datasets=[
    {
        "folder_name" : "orders"
    },
    {
        "folder_name" : "products"
    },
    {
        "folder_name" : "customers"
    }   
]

# COMMAND ----------

import json
datasets_json_string = json.dumps(datasets)

dbutils.jobs.taskValues.set("output_datasets", datasets_json_string)

# COMMAND ----------

