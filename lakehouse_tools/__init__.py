# python
import pathlib
from typing import List, Union, Dict

# spark
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat_ws, count, md5, row_number


def return_df(df: DataFrame) -> DataFrame:

    """
    Sample function that returns the given DataFrame.

    :param df: A Spark DataFrame
    :type df: DataFrame

    :returns: Returns the Spark DataFrame that has been put in.
    :rtype: DataFrame
    """

    return df