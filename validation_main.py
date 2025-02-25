# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as F
import datetime

# COMMAND ----------

def read_data(type, connection, query):
    try:
        if type == "databricks":
            df = spark.sql(query)
            df = df.withColumn("unique_key", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in df.columns]), 256))
            return df
        elif type == "jdbc":
            connection_string = dbutils.secrets.get(scope="my_scope", key=connection)
            df = spark.read.format("jdbc").option("url", connection_string).option("query", query).load()
            df = df.withColumn("unique_key", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in df.columns]), 256))
            return df
        return spark.createDataFrame([("default_unique_key",)], StructType([StructField("unique_key", StringType(), True)]))
    except Exception as e:
        print(f"Error: {e}")
        return spark.createDataFrame([("default_unique_key",)], StructType([StructField("unique_key", StringType(), True)]))

# COMMAND ----------

metadata = spark.sql("SELECT * FROM catalog_name.default.validation_metadata_table")

schema = StructType([
    StructField("metadata_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_count", IntegerType(), True),
    StructField("target_count", IntegerType(), True),
    StructField("test_1", StringType(), True),
    StructField("test_2", StringType(), True),
    StructField("test_3", StringType(), True),
    StructField("created_dt", TimestampType(), True)
])

for row in metadata.collect():
    df_source = read_data(row.source_type, row.source_connection, row.source_query)
    df_target = read_data(row.target_type, row.target_connection, row.target_query)

    # test 1: count check
    source_count = df_source.count()
    target_count = df_target.count()

    status = test_1 = test_2 = test_3 = "Success"
    if source_count != target_count:
        status = "Failure"
        test_1 = f"Source and target rows don't match: {source_count} == {target_count}"

    # test 2: value check
    df_joined = df_source.alias('src').join(df_target.alias('tgt'), F.col('src.unique_key') == F.col('tgt.unique_key'), 'fullouter')

    match_count = df_joined.filter(F.col('src.unique_key').isNotNull() & F.col('tgt.unique_key').isNotNull()).count()
    not_found_source = df_joined.filter(F.col('src.unique_key').isNull()).count()
    not_found_target = df_joined.filter(F.col('tgt.unique_key').isNull()).count()

    if(not_found_source > 0 or not_found_target > 0):
        status = "Failure"
        test_2 = f"Rows match: {match_count}, Not found in source: {not_found_source}, Not found in target: {not_found_target}"

    # test 3: duplicate check
    source_duplicates = df_source.groupBy("unique_key").count().filter(F.col("count") > 1).count()
    target_duplicates = df_target.groupBy("unique_key").count().filter(F.col("count") > 1).count()

    if source_duplicates > 0 or target_duplicates > 0:
        status = "Failure"
        test_3 = f"Source duplicates: {source_duplicates}, Target duplicates: {target_duplicates}"

    validation_data = [
        Row(
            metadata_id=row.metadata_id,
            status=status,
            source_count=source_count, 
            target_count=target_count, 
            test_1=test_1,
            test_2=test_2,
            test_3=test_3,
            created_dt=datetime.datetime.now()
        )
    ]

    validation_df = spark.createDataFrame(validation_data, schema)
    validation_df.write.mode("append").saveAsTable("catalog_name.default.validation_metrics_table")