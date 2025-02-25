# Data Validation Documentation for Source and Target Tables in Databricks

## 1. Overview

This documentation details the validation process for ensuring data consistency between any source/target tables in the Databricks environment. The validation process is managed using a set of Databricks Notebooks that perform data quality checks based on metadata-driven execution.

## 2. Components

### 2.1 Validation Metadata Notebook

The `validation_metadata` notebook is responsible for creating and populating the metadata table that stores validation rules.

- **Table Created**: `catalog_name.default.validation_metadata_table`
- **Columns**:
  - `metadata_id` – Primary Key for this table.
  - `source_type` – Type of source (Databricks SQL with Unity Catalog, Hive Metastore, JDBC table, or other supported data types).
  - `source_connection` – Required for some data sources; connection string needs to be stored in Databricks scope/secret vault.
  - `source_query` – SQL query for source table validation.
  - `target_type` – Type of target data source.
  - `target_connection` – Required if the target data source needs a connection string.
  - `target_query` – SQL query for target table validation.
  - **Note**: The source and target queries must return columns in the same order and with identical names for proper validation.

## 3. Validation Process

The validation process iterates through records stored in `validation_metadata_table`, executes source and target queries, and performs three types of validation checks:

### 3.1 Data Reading Function

A function `read_data` fetches data from Databricks or JDBC sources based on metadata:

- Reads data from the specified source using Spark SQL.
- Generates a `unique_key` column using SHA-256 hashing to identify unique records.

### 3.2 Validation Checks

- **Row Count Validation (Test 1)**
  - Compares the number of rows in the source and target tables.
  - If the row counts mismatch, the validation fails. _(Source and target rows don't match.)_
- **Data Consistency Validation (Test 2)**
  - Joins the source and target datasets using the `unique_key`.
  - Identifies unmatched records.
  - If there are missing records in either source or target, the validation fails. _(Source and target rows don't match.)_
- **Duplicate Records Check (Test 3)**
  - Identifies duplicate `unique_key` records in source and target tables.
  - If duplicates exist, the validation fails. _(Source and target rows don't match.)_

### 3.3 Validation Metrics Table

The validation results are stored in `catalog_name.default.validation_metrics_table` with the following schema:

- `metadata_id` – Foreign Key referencing the metadata table.
- `status` – Indicates Success/Failure (Success if all three tests pass, Failure if any test fails).
- `source_count` – Row count of the source query.
- `target_count` – Row count of the target query.
- `test_1` – Result of row count validation (Success or error message).
- `test_2` – Result of data consistency validation (Success or error message).
- `test_3` – Result of duplicate records validation (Success or error message).
- `created_dt` – Timestamp of when the validation was executed.

Each validation result is appended to this table for tracking purposes.

## 4. Execution Flow

- **Populate Metadata Table:**
  - Insert validation rules into `validation_metadata_table`.
- **Read and Execute Queries:**
  - Fetch metadata records and execute source and target queries.
- **Perform Validation Checks:**
  - Row count validation.
  - Data consistency validation.
  - Duplicate records validation.
- **Store Validation Results:**
  - Append validation results to `validation_metrics_table`.
- **Analyze and Resolve Issues:**
  - Review failed validations and take corrective actions.

## 5. Sample Validation Queries

The following queries validate the data consistency between source and target tables:

**Example for `validation_metadata_table`:**

```sql
INSERT INTO catalog_name.default.validation_metadata_table
(metadata_id, source_type, source_connection, source_query, target_type, target_connection, target_query)
VALUES
(1, 'Databricks SQL', NULL, 'SELECT * FROM source_table', 'Databricks SQL', NULL, 'SELECT * FROM target_table');
```

## 6. Conclusion

This validation framework ensures data integrity between source and target tables. The metadata-driven approach allows for scalable and dynamic validation with minimal manual intervention.
