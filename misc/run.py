# python
import sys, shutil, os

# spark
import pyspark
from delta import DeltaTable, configure_spark_with_delta_pip
from datetime import datetime as dt
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DateType,
    TimestampType,
)

import lakehouse_tools as lt

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", "2")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# ----- get_system_info ----- #

lt.get_system_info(spark)


# ----- create_sample_df ----- #

lt.create_sample_df(spark).show()
lt.create_sample_df(spark, add_nulls=True).show()
lt.create_sample_df(spark, wide=True).show()
lt.create_sample_df(spark, wide=True, add_nulls=True).show()


# ----- write_delta_table_for_path ----- #

temp_dir = f"{os.getcwd()}/temp"
shutil.rmtree(temp_dir)

df = lt.create_sample_df(spark)
lt.write_delta_table_for_path(df = df, path = f"{temp_dir}/sample_df", mode = "append", overwriteSchema = False, mergeSchema = False)


# ----- get_columns_matching_string ----- #

lt.get_columns_matching_string(df = df, match_string = "int", match_mode = "contains", ignore_case = True)
lt.get_columns_matching_string(df = df, match_string = "id", match_mode = "contains", ignore_case = False)
lt.get_columns_matching_string(df = df, match_string = "Id", match_mode = "contains", ignore_case = True)
lt.get_columns_matching_string(df = df, match_string = "STR", match_mode = "starts_with", ignore_case = True)
lt.get_columns_matching_string(df = df, match_string = "STR", match_mode = "starts_with", ignore_case = False)
lt.get_columns_matching_string(df = df, match_string = "col", match_mode = "ends_with", ignore_case = True)
lt.get_columns_matching_string(df = df, match_string = "booleancol", match_mode = "ends_with", ignore_case = True)


lt.replace_column_name_substring(df = df, existing_substring = "int", replacement_substring = "newName", replacement_dict = None).show()
lt.replace_column_name_substring(df = df, existing_substring = "floatCol", replacement_substring = "newNameCol", replacement_dict = None).show()
lt.replace_column_name_substring(df = df, replacement_dict = {"stringIdCol": "string_id_col", "booleanCol": "boolean_col"}).show()


# ----- add_missing_columns ----- #

df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["stringCol", "intCol"])
df = lt.add_missing_columns(df, column_list = df.columns, keep_unknown_cols = True)
df.dtypes


data = [(None, 1, None),
        (None, 2, None),
        (None, 3, None),
        (None, 4, 5)]

schema = StructType([
    StructField("allNull", StringType()),
    StructField("noNull", IntegerType()),
    StructField("partialNull", IntegerType())
])

df = spark.createDataFrame(data, schema)

# Call the function to get columns with all null values
null_columns = lt.get_null_column_names(df)
print(null_columns)


# ----- write_delta_table_for_path ----- #

temp_dir = f"{os.getcwd()}/temp"
shutil.rmtree(temp_dir)

df = lt.create_sample_df(spark)
lt.write_delta_table_for_path(df = df, path = f"{temp_dir}/df_sample", mode = "append", overwriteSchema = False, mergeSchema = False)
lt.get_file_count(path = temp_dir)
lt.get_file_count(path = temp_dir, recursive=True)
lt.get_file_count(path = temp_dir, file_format="crc", recursive=True)
lt.get_file_count(path = temp_dir, file_format="crc", recursive=True, ignore_dirs=["_delta_log"])