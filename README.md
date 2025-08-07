# Medallion-Data-Warehouse-on-Azure-with-Databricks-PySpark
This project demonstrates the design and implementation of a Medallion-oriented data warehouse on the Azure cloud platform. I developed an end-to-end data pipeline using PySpark notebooks to process and transform sample data, creating a structured data lake for analytics and reporting.

#Key Components and Technologies

Architecture: Medallion Architecture (Bronze, Silver, Gold layers)

Platform: Azure Data Lake Storage, Azure Databricks, Azure Data Factory (ADF)

Processing: PySpark, DataFrames, Spark SQL, ADF

Data: Sample customer, order, product, and region data

Pipelines: ETL (Extract, Transform, Load) and ELT (Extract, Load, Transform) methodologies

Business Value
This architecture provides a scalable and robust framework for data governance. By progressively refining raw data into a clean, curated format, it ensures data quality and consistency for downstream business intelligence and machine learning applications. The use of a Medallion structure enables auditability and simplifies data recovery.
