# python
import sys

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


lt.get_system_info(spark)

lt.create_sample_df(spark).show()
lt.create_sample_df(spark, add_nulls=True).show()
lt.create_sample_df(spark, wide=True).show()
lt.create_sample_df(spark, wide=True, add_nulls=True).show()

