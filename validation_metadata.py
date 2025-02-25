# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_name.default.validation_metadata_table
# MAGIC (
# MAGIC   metadata_id STRING,
# MAGIC   source_type STRING,
# MAGIC   source_connection STRING,
# MAGIC   source_query STRING,
# MAGIC   target_type STRING,
# MAGIC   target_connection STRING,
# MAGIC   target_query STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into catalog_name.default.validation_metadata_table (
# MAGIC   metadata_id,
# MAGIC   source_type,
# MAGIC   source_connection,
# MAGIC   source_query,
# MAGIC   target_type,
# MAGIC   target_connection,
# MAGIC   target_query
# MAGIC ) values (
# MAGIC   1,
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SELECT name, code, count(*) as count FROM  source_schema.source_table group by name, code",
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SELECT name, code, count(*) as count FROM target_schema.target_table group by name, code"
# MAGIC ), (
# MAGIC   2,
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SHOW COLUMNS IN  source_schema.source_table",
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SHOW COLUMNS IN target_schema.target_table"
# MAGIC ), (
# MAGIC   3,
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SELECT  approver_Name, approver_EmployeeID, approver_UserID, count(*) as count 
# MAGIC   FROM  source_schema.source_table 
# MAGIC   group by approver_Name, approver_EmployeeID, approver_UserID",
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SELECT  approver_Name, approver_EmployeeID, approver_UserID, count(*) as count 
# MAGIC   FROM target_schema.target_table
# MAGIC   group by approver_Name, approver_EmployeeID, approver_UserID"
# MAGIC ), (
# MAGIC   4,
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SELECT distinct currency_Code, documentation_ID,  count(*) as count
# MAGIC   FROM  source_schema.source_table
# MAGIC   group by currency_Code, documentation_ID",
# MAGIC   "databricks",
# MAGIC   "",
# MAGIC   "SELECT distinct  currency_Code, documentation_ID,  count(*) as count
# MAGIC   FROM target_schema.target_table
# MAGIC   group by  currency_Code, documentation_ID"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_name.default.validation_metrics_table
# MAGIC (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   metadata_id STRING,
# MAGIC   status STRING,
# MAGIC   source_count INT,
# MAGIC   target_count INT,
# MAGIC   test_1 STRING,
# MAGIC   test_2 STRING,
# MAGIC   test_3 STRING,
# MAGIC   created_dt TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog_name.default.validation_metadata_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog_name.default.validation_metrics_table