# python
import pytest

# spark
import chispa
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

from lakehouse_tools import *

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


# return_df
def test_return_df_normal():

    data = [
        (1, "A", True, dt(2019, 1, 1), None),
        (2, "B", True, dt(2019, 1, 1), None),
        (4, "D", True, dt(2019, 1, 1), None),
    ]

    schema = StructType(
        [
            StructField("pkey", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("effective_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
        ]
    )

    df = spark.createDataFrame(data=data, schema=schema)
    result_df = return_df(df = df)
    expected_df = spark.createDataFrame(data=data, schema=schema)

    chispa.assert_df_equality(result_df, expected_df, ignore_row_order=True)


# is_delta_table
def test_is_delta_table(tmp_path):

    # https://docs.pytest.org/en/latest/how-to/tmp_path.html
    path = f"{tmp_path}/delta/simple_df"
    data = [
        (1, "A", True, dt(2019, 1, 1), None),
        (2, "B", True, dt(2019, 1, 1), None),
        (4, "D", True, dt(2019, 1, 1), None),
    ]

    schema = StructType(
        [
            StructField("pkey", IntegerType(), True),
            StructField("attr", StringType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("effective_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
        ]
    )

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.format("delta").save(path)

    #delta_table = DeltaTable.forPath(spark, path)
    saved_table = DeltaTable.forPath(spark, path)

    assert is_delta_table(path) == True