# python
import pathlib, os
from typing import List, Union, Dict, Optional

# spark
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat_ws, count, md5, row_number
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    TimestampType,
    ArrayType
    )

def return_df(df: DataFrame) -> DataFrame:

    """
    Sample function that returns the given DataFrame.

    :param df: A Spark DataFrame
    :type df: DataFrame

    :returns: Returns the Spark DataFrame that has been put in.
    :rtype: DataFrame
    """

    return df


def is_delta_table(path: str) -> bool:
    
    """
    Simple test if a given path is a Delta table path.

    :param path: A Delta table directory path
    :type path: str

    :returns: Returns True if it is Delta table, else False
    :rtype: bool
    """

    dir = pathlib.Path(path)
    delta_log_dir = dir / "_delta_log"

    if delta_log_dir.exists():
        result = True
    else:
        result = False
    
    return result


def get_system_info(spark: SparkSession):

    major, minor, patch = spark.version.split('.')

    system_info = {
        "databricks": "DB_HOME" in os.environ,
        "spark_version_major": int(major),
        "spark_version_minor": int(minor),
        "spark_version_patch": int(patch),
        "spark_version": f"{major}.{minor}.{patch}",
        "runtime_version": os.environ.get('DATABRICKS_RUNTIME_VERSION', None)
    }

    return system_info


def create_sample_df(spark: SparkSession, wide: Optional[bool] = False, add_nulls: Optional[bool] = False):

    schema_small = StructType([
        StructField("stringIdCol", StringType()),
        StructField("stringCol", StringType()),
        StructField("intCol", IntegerType()),
        StructField("floatCol", FloatType()),
        StructField("booleanCol", BooleanType())
    ])

    schema_wide = StructType([
        StructField("stringIdCol", StringType()),
        StructField("stringCol", StringType()),
        StructField("intCol", IntegerType()),
        StructField("floatCol", FloatType()),
        StructField("booleanCol", BooleanType()),
        StructField("stringIntCol", StringType()),
        StructField("stringWithIntCol", StringType()),
        StructField("stringBooleanCol", StringType()),
        StructField("stringBooleanLowerCaseCol", StringType()),
        StructField("stringAllNullCol", StringType()),
        StructField("arrayStringSingleElementCol", ArrayType(StringType())),
        StructField("arrayStringMultipleElementsCol", ArrayType(StringType())),
        StructField("dictStringOneLevelSingleKeyCol", StructType([
            StructField("key1", StringType())
            ])
        ),
        StructField("dictStringOneLevelMultipleKeysCol", StructType([
            StructField("key1", StringType()),
            StructField("key2", StringType())
            ])
        ),
        StructField("dictStringTwoLevelMultipleKeysCol", StructType([
            StructField("key1_0", StructType([
                StructField("key1_1", StringType())
                ])
            ),
            StructField("key2_0", StructType([
                StructField("key2_1", StringType())
                ])
            )
            ])
        ),
        StructField("dictStringOneLevelSingleKeyStringArrayCol", StructType([
            StructField("key1", ArrayType(StringType()))
            ])
        ),
        StructField("dictStringOneLevelMultipleKeysStringArrayCol", StructType([
            StructField("key1", ArrayType(StringType())),
            StructField("key2", StringType())
            ])
        ),
        StructField("dictStringTwoLevelMultipleKeysStringArrayCol", StructType([
            StructField("key1_0", StructType([
                StructField("key1_1", ArrayType(StringType()))
                ])),
            StructField("key2_0", StructType([
                StructField("key2_1", StringType())
                ]))
            ])
        ),
        StructField("arrayDictStringSingleLevelSingleKeyCol", ArrayType(
            StructType([
                StructField("key1", StringType())
                ])
            )
        ),
        StructField("arrayDictStringSingleLevelMultipleKeysCol", ArrayType(
            StructType([
                StructField("key1", StringType()),
                StructField("key2", StringType())
                ])
            )
        ),
        StructField("arrayDictStringTwoLevelMultipleKeysCol", ArrayType(
            StructType([
                StructField("key1_0", StructType([
                    StructField("key1_1", StringType())
                    ])
                ),
                StructField("key2_0", StructType([
                    StructField("key2_1", StringType())
                    ])
                )
            ])
            )
        )
    ])

    data_dict = {
        "stringIdCol": ["1", "2", "3"],
        "stringCol": ["a", "b", "c"],
        "intCol": [1, 2, 3],
        "floatCol": [1.0, 2.0, 3.0],
        "booleanCol": [True, False, True],
        "stringIntCol": ["1", "2", "3"],
        "stringWithIntCol": ["My Street 1", "Your street 2", "All Street 3"],
        "stringBooleanCol": ["True", "False", "True"],
        "stringBooleanLowerCaseCol": ["true", "false", "true"],
        "stringAllNullCol": [None, None, None],
        "arrayStringSingleElementCol": [["a"], ["b"], ["c"]],
        "arrayStringMultipleElementsCol": [["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"]],
        "dictStringOneLevelSingleKeyCol": [{"key1": "value1"}, {"key1": "value1"}, {"key1": "value1"}],
        "dictStringOneLevelMultipleKeysCol": [
            {"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}],
        "dictStringTwoLevelMultipleKeysCol": [
            {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}}, 
            {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
            {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}}
            ],
        "dictStringOneLevelSingleKeyStringArrayCol": [{"key1": ["a", "b", "c"]}, {"key1": ["a", "b", "c"]}, {"key1": ["a", "b", "c"]}],
        "dictStringOneLevelMultipleKeysStringArrayCol": [
            {"key1": ["a", "b", "c"], "key2": "value2"}, 
            {"key1": ["a", "b", "c"], "key2": "value2"}, 
            {"key1": ["a", "b", "c"], "key2": "value2"}],
        "dictStringTwoLevelMultipleKeysStringArrayCol": [
            {"key1_0": {"key1_1": ["a", "b", "c"]}, "key2_0": {"key2_1": "value2_1"}}, 
            {"key1_0": {"key1_1": ["a", "b", "c"]}, "key2_0": {"key2_1": "value2_1"}},
            {"key1_0": {"key1_1": ["a", "b", "c"]}, "key2_0": {"key2_1": "value2_1"}}
            ],
        "arrayDictStringSingleLevelSingleKeyCol": [
            [{"key1": "value1"}, {"key1": "value1"}, {"key1": "value1"}],
            [{"key1": "value1"}, {"key1": "value1"}, {"key1": "value1"}],
            [{"key1": "value1"}, {"key1": "value1"}, {"key1": "value1"}]
            ],
        "arrayDictStringSingleLevelMultipleKeysCol": [
            [{"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}],
            [{"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}],
            [{"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}]
            ],
        "arrayDictStringTwoLevelMultipleKeysCol": [
            [
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}}
            ],
            [
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}}
            ],
            [
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}}
            ]
            ]
        }

    data_dict_none = {
        "stringIdCol": ["4", "5", "6"],
        "stringCol": ["a", "b", None], 
        "intCol": [1, 2, None],
        "floatCol": [1.0, 2.0, None],
        "booleanCol": [True, False, True],
        "stringIntCol": ["1", "2", None],
        "stringWithIntCol": ["My Street 1", "Your street 2", None],
        "stringBooleanCol": ["True", "False", None],
        "stringBooleanLowerCaseCol": ["true", "false", None],
        "stringAllNullCol": [None, None, None],
        "arrayStringSingleElementCol": [["a"], ["b"], None],
        "arrayStringMultipleElementsCol": [["a", "b", "c"], ["a", "b", None], None],
        "dictStringOneLevelSingleKeyCol": [{"key1": "value1"}, {"key1": None}, None],
        "dictStringOneLevelMultipleKeysCol": [
            {"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": None}, None],
        "dictStringTwoLevelMultipleKeysCol": [
            {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": None}}, 
            {"key1_0": {"key1_1": "value1_1"}, "key2_0": None},
            None
            ],
        "dictStringOneLevelSingleKeyStringArrayCol": [{"key1": ["a", "b", "c"]}, {"key1": ["a", "b", None]}, {"key1": None}],
        "dictStringOneLevelMultipleKeysStringArrayCol": [
            {"key1": ["a", "b", None], "key2": "value2"}, 
            {"key1": None, "key2": "value2"}, 
            None
            ],
        "dictStringTwoLevelMultipleKeysStringArrayCol": [
            {"key1_0": {"key1_1": ["a", "b", None]}, "key2_0": {"key2_1": "value2_1"}}, 
            {"key1_0": {"key1_1": ["a", "b", "c"]}, "key2_0": None},
            None
            ],
        "arrayDictStringSingleLevelSingleKeyCol": [
            [{"key1": "value1"}, {"key1": "value1"}, {"key1": None}],
            [{"key1": "value1"}, {"key1": "value1"}, None],
            None
            ],
        "arrayDictStringSingleLevelMultipleKeysCol": [
            [{"key1": "value1", "key2": "value2"}, {"key1": None, "key2": "value2"}, {"key1": "value1", "key2": None}],
            [{"key1": "value1", "key2": "value2"}, {"key1": "value1", "key2": "value2"}, None],
            None
            ],
        "arrayDictStringTwoLevelMultipleKeysCol": [
            [
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": "value2_1"}}
            ],
            [
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": {"key2_1": None}},
                {"key1_0": {"key1_1": "value1_1"}, "key2_0": None},
                None
            ],
            None
            ]
        }

    data_dict_small = {
        "stringIdCol": ["1", "2", "3"],
        "stringCol": ["a", "b", "c"],
        "intCol": [1, 2, 3],
        "floatCol": [1.0, 2.0, 3.0],
        "booleanCol": [True, False, True]
    }

    data_dict_small_none = {
        "stringIdCol": ["1", "2", None],
        "stringCol": ["a", "b", None],
        "intCol": [1, 2, None],
        "floatCol": [1.0, 2.0, None],
        "booleanCol": [True, False, None]
    }

    if wide:
        df = spark.createDataFrame(list(zip(*data_dict.values())), schema_wide)
        if add_nulls:
            df_none = spark.createDataFrame(list(zip(*data_dict_none.values())), schema_wide)
            df = df.union(df_none)
    else:
        df = spark.createDataFrame(list(zip(*data_dict_small.values())), schema_small)
        if add_nulls:
            df_none = spark.createDataFrame(list(zip(*data_dict_small_none.values())), schema_small)
            df = df.union(df_none)     

    return df