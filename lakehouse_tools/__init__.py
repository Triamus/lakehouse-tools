# python
import pathlib, os, re
from typing import List, Union, Dict, Optional

# spark
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit
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
        StructField("dictIntFloatOneLevelMultipleKeysCol", StructType([
            StructField("key1", IntegerType()),
            StructField("key2", FloatType())
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
        "dictIntFloatOneLevelMultipleKeysCol": [
            {"key1": 1, "key2": 1.1}, {"key1": 2, "key2": 2.2}, {"key1": 3, "key2": 3.3}],
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
        "dictIntFloatOneLevelMultipleKeysCol": [
            {"key1": 1, "key2": 1.1}, {"key1": 2, "key2": None}, None],
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
        "stringIdCol": ["4", "5", None],
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


def write_delta_table_for_path(df, path, mode = "append", overwriteSchema = False, mergeSchema = False):

    if overwriteSchema:
        overwriteSchema = "true"
    else:
        overwriteSchema = "false"

    if mergeSchema:
        mergeSchema = "true"
    else:
        mergeSchema = "false"

    (df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", overwriteSchema)
        .option("mergeSchema", mergeSchema)
        .option("path", path)
        .save())
    

def get_columns_matching_string(
    df: DataFrame, 
    match_string: str, 
    match_mode: str,
    ignore_case: bool = True) -> List[str]:

    """
    Get column list from a PySpark DataFrame based on the specified match mode.

    Parameters:
    - df (DataFrame): PySpark DataFrame.
    - match_string (str): String to match columns against.
    - match_mode (str): One of "starts_with", "ends_with", or "contains".
    - ignore_case (bool, optional): Whether to perform case-insensitive matching (default is True).

    Returns:
    - List[str]: List of strings with identified columns.
    """
    allowed_match_modes = ["starts_with", "ends_with", "contains"]
    if not match_mode in allowed_match_modes:
        raise Exception(f"Argument match_mode needs to be one of {', '.join(allowed_match_modes)}")

    if ignore_case:
        match_string = match_string.lower()

    escaped_match_string = re.escape(match_string)

    selected_columns = []
    for col_name in df.columns:
        if ignore_case:
            col_name = col_name.lower()
            if (match_mode == "starts_with" and col_name.startswith(escaped_match_string)) or \
               (match_mode == "ends_with" and col_name.endswith(escaped_match_string)) or \
               (match_mode == "contains" and re.search(escaped_match_string, col_name, re.IGNORECASE)):
                selected_columns.append(col_name)
            
        else:
            if (match_mode == "starts_with" and col_name.startswith(escaped_match_string)) or \
               (match_mode == "ends_with" and col_name.endswith(escaped_match_string)) or \
               (match_mode == "contains" and re.search(escaped_match_string, col_name)):
                selected_columns.append(col_name)
    
    if ignore_case:
        selected_columns = [col for col in df.columns if col.lower() in selected_columns]

    return selected_columns


def replace_column_name_substring(
    df: DataFrame,
    existing_substring: Optional[str] = None,
    replacement_substring: Optional[str] = None,
    replacement_dict: Optional[Dict[str, str]] = None
) -> DataFrame:
    """
    Update column names in a PySpark DataFrame by replacing an existing substring.

    Args:
        df (DataFrame): The PySpark DataFrame to update.
        existing_substring (str, optional): The existing column name substring to replace.
            Not required if replacement_dict is provided.
        replacement_substring (str, optional): The replacement column name substring.
            Not required if replacement_dict is provided.
        replacement_dict (dict, optional): A dictionary where keys are existing substrings and
            values are replacement substrings.

    Returns:
        DataFrame: The PySpark DataFrame with updated column names.
    """
    if replacement_dict is not None:
        new_column_names = []
        for col in df.columns:
            new_col = col
            for existing, replacement in replacement_dict.items():
                new_col = new_col.replace(existing, replacement)
            new_column_names.append(new_col)
    elif existing_substring is not None and replacement_substring is not None:
        new_column_names = [col.replace(existing_substring, replacement_substring) for col in df.columns]
    else:
        raise ValueError("Either replacement_dict or both existing_substring and replacement_substring must be provided.")

    select_expr = [f"`{col}` as `{new_col}`" for col, new_col in zip(df.columns, new_column_names)]
    updated_df = df.selectExpr(*select_expr)

    return updated_df


def add_missing_columns(df: DataFrame, column_list: list, keep_unknown_cols: bool = True) -> DataFrame:
    
    missing_columns = [col for col in column_list if col not in df.columns]
    additional_columns = [col for col in df.columns if col not in column_list]
    
    df_new = df

    for col in missing_columns:
        df_new = df_new.withColumn(col, lit(None))

    if keep_unknown_cols and (len(additional_columns) > 0):
        column_list.extend(additional_columns)

    return df_new.select([f"`{col}`" for col in column_list])


def get_null_column_names(df: DataFrame) -> list:
    """
    Get the names of columns in a PySpark DataFrame that contain all null values.

    Args:
        df (DataFrame): The input PySpark DataFrame.

    Returns:
        list: A list of column names with all null values.
    """
    null_columns = []

    for column in df.columns:
        non_null_count = df.where(col(column).isNotNull()).count()
        if non_null_count == 0:
            null_columns.append(column)

    return null_columns


def get_file_count(path: str, file_format: Optional[str] = None, recursive: bool = False, ignore_dirs: List[str] = []) -> int:
    """
    Count the number of files in a directory, optionally filtering by file format and
    including files from subdirectories if the 'recursive' flag is set to True.

    Args:
        path (str): The path to the directory.
        file_format (str, optional): If provided, only count files with the given format (e.g., "txt").
        recursive (bool, optional): If True, also count files from subdirectories. Default is False.
        ignore_dirs (List[str], optional): A list of subdirectory names to ignore during recursive search.

    Returns:
        int: The number of files in the directory (including subdirectories if 'recursive' is True).
    """
    path = pathlib.Path(path)

    file_count = 0
    for item in path.iterdir():
        if item.is_file():
            if file_format is None or item.suffix == f".{file_format}":
                file_count += 1

        elif recursive and item.is_dir() and item.name not in ignore_dirs:
            file_count += get_file_count(item, file_format, recursive=True, ignore_dirs=ignore_dirs)

    return file_count