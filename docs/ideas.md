<h1 style="font-size:40px;"><b>Lakehouse Tools Ideas</b></h1>


# Overview

This document serves as a temporary, low-friction place to collect ideas around 

- functions / methods
- package design
- CI/CD
- project strategy, goals, scope, etc.

It is not meant to be a planning tool. The idea is to quickly document an idea here before moving it to a proper place like an issue. There is no guidance as to how to document an idea. One may find inspiration from existing items.

<div style="page-break-after: always;"></div>


# Databricks

## Data lake mount and unmount

**author**

aw

**Overview**

Functions that mount or unmount a data lake container.

**Goal**

Safely mount or unmount an Azure data lake container without errors and proper exception handling.

**Implementation**

There may be more than one way to mount a container, e.g. with service principal or as AD passthrough so it needs to be investigated which scenarios to support and if multiple functions make more sense. Target language is Python.

**References**

There are some existing notebooks.

- https://diangermishuizen.com/function-to-mount-a-storage-account-container-to-azure-databricks/

**Effort**

2 hours per function

**Impact**

Impact should be medium as mounts are perfromed frequently and errors occur in several, preventable cases

<div style="page-break-after: always;"></div>


# Spark

<div style="page-break-after: always;"></div>


# Delta

## Delta table check

**author**

aw

**Overview**

A function which takes in a table path and checks if it is a delta table.

**Goal**

Most tables in the data lake should be delta tables but they may not be e.g. because they have been created by some process which cannot write delta format, because they have been copied from somewhere else, etc. Rather than doing some manual check, a generic function would streamline it.

**Implementation**

The function should return a boolean true or false. Dependencies should be kept to minimum. A Python implementation should suffice but spark sql may be possible. A few ideas

- check for presence of `_delta_log` directory
- think about edge cases e.g. could there be a `_delta_log` directory but no table or the table is not delta any longer
- what happens to shallow clones; do they have _delta_log dir

**References**

- [Databricks Blog: Diving Into Delta Lake: Unpacking The Transaction Log](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)

**Effort**

Maybe: 4 hours

**Impact**

Impact is probably small as the check is (currently) not performed too often.

<div style="page-break-after: always;"></div>


## Remove duplicates from Delta table

**Overview**

A function which takes in one or more columns of a delta table and removes all duplicate rows.

**Goal**

Remove all duplicate rows from a delta table based on selected columns. An example is below.

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A| # duplicate
|   2|   A|   B|
|   3|   A|   A| # duplicate
|   4|   A|   A| # duplicate
|   5|   B|   B| # duplicate
|   6|   D|   D|
|   9|   B|   B| # duplicate
+----+----+----+

After de-duplication of columns col2 and col3, the table should look as below.

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   2|   A|   B|
|   6|   D|   D|
+----+----+----+

**Implementation**

Target API is PySpark. We only target delta tables as the most common data lake table format. Some notes below.

- maybe use a hash value

**References**

- [GitHub/MrPowers/mack: kill_duplicates function](https://github.com/MrPowers/mack#kill-duplicates)
  - note that this function removes all duplicates
- [GitHub/MrPowers/mack: drop_duplicates_pkey function](https://github.com/MrPowers/mack#drop-duplicates-with-primary-key)
- [GitHub/MrPowers/mack: drop_duplicates function](https://github.com/MrPowers/mack#drop-duplicates)

**Effort**

Maybe: 6 hours

**Impact**

Impact could be medium as this operation may not be uncommon.

<div style="page-break-after: always;"></div>


## Delta table size

**Overview**

A rough indication of the total (current) delta table file size. If possible with or without historic (time travel) data.

**Goal**

Give an indication of delta table file size which has multiple applications.

**Implementation**

Maybe this can be read from metadata, if not, probably needs to be computed by some file system operation. Potentially needs a helper function for human readability of byte return value, see e.g. [GitHub/MrPowers/mack: humanize_bytes function](https://github.com/MrPowers/mack#humanize-bytes).

**References**

- [GitHub/MrPowers/mack: delta_file_sizes function](https://github.com/MrPowers/mack#delta-file-sizes)

**Effort**

maybe: 6 hours

**Impact**

Small to medium impact but should support automation jobs and data size awareness especially for business users.

<div style="page-break-after: always;"></div>


# Template 

## Your feature name here

**author**

author

**Overview**

description of idea in high-level terms

**Goal**

what this feature could solve

**Implementation**

how this feature could be implemented; please mention the target language (if applicable) e.g. Python, PySpark, Spark SQL, Scala, etc.

**References**

articles, blogs, books, videos, documentation, reference implementations (e.g. on GitHub, maybe in another lnaguage), etc.

**Effort**

(optional): how much effort an end2end implementation could take

**Impact**

(optional): impact evaluation how much the feature may save time, costs, etc.

<div style="page-break-after: always;"></div>


# Unprocessed ideas

- aw: general: databricks db demos for inspiration and code
  - https://github.com/databricks-demos/dbdemos
  - https://www.dbdemos.ai/
- aw: general: delta lake utilities: https://docs.delta.io/latest/delta-utility.html#language-python
- aw: general spark problems code
  - https://github.com/subhamkharwal/ease-with-apache-spark/
- aw: filepath exists
- aw: safely delete delta tables (directories) from data lake
  - probably need handling of AD passthrough vs mounts vs UC-style
- aw: python: more file system ops
  - https://towardsdatascience.com/goodbye-os-path-15-pathlib-tricks-to-quickly-master-the-file-system-in-python-881213ca7c21
- aw: databricks runtime check
- aw: cluster/hardware check
- aw: Identify uniqueness of value/key 
- aw: validate presence or absence of cols in df
- aw: make df cols snake case
- aw: sort cols according to target list; list may not incl all cols
- aw: rename cols to target names from dict; dict may not include all cols
- aw: two cols to dict in df
- aw: df to list of dicts
- aw: get all databricks jobs with some extra data
  - https://github.com/souvik-databricks/utilities/blob/main/sql_job_identifier.ipynb
	- https://github.com/souvik-databricks/utilities/blob/main/sql_job_identifierv2.ipynb
- aw: date/time/timestamp functions
  - timestamp to string with various formats
  - timestamp to date
  - date to string with various formats
  - change date format
- aw: count by group
- aw: for more ideas around delta and hive, see below
  - https://github.com/MrPowers/jodie
  - https://github.com/MrPowers/mack
  - https://github.com/MrPowers/eren (not active yet)
  - https://github.com/christophergrant/hydro
  - https://github.com/databrickslabs/migrate
- aw: for more ideas around databricks platform, see below
  - Scripts to help customers with one-off migrations between Databricks workspaces.: https://github.com/databrickslabs/migrate
  - GitHub/hwang-db/db-common-assets (To store quick starters for daily tasks on Databricks): https://github.com/hwang-db/db-common-assets
    - example: https://github.com/hwang-db/db-common-assets/blob/main/adb-bootcamp-data-engineer-20220616/ADB-bootcamp-20220616/ADB-bootcamp-20220616/labs/data-engineering-1.3.0/Python/classic/includes/main/python/operations.py
- aw: hive / databricks sql object ownership chain
- aw: df count by cols
- aw: copy delta table (see mack but also delta clone)
- aw: drop columns in delta table
  - https://delta.io/blog/2022-08-29-delta-lake-drop-column/
- odbc and jdbc reader/writer wrappers
  - https://stackoverflow.com/questions/66675003/java-sql-sqlexception-no-suitable-driver-when-tryingt-to-run-a-python-script
  - https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/
- aw: also look into https://github.com/databricks/databricks-sql-python for inspiration around table functions etc.
- aw: json flattening
  - https://medium.com/@thomaspt748/how-to-flatten-json-files-dynamically-using-apache-pyspark-c6b1b5fd4777
  - https://www.databricks.com/blog/2022/09/07/parsing-improperly-formatted-json-objects-databricks-lakehouse.html
  - https://towardsdatascience.com/json-in-databricks-and-pyspark-26437352f0e9
- aw: database upsert
  - https://medium.com/@thomaspt748/how-to-upsert-data-into-a-relational-database-using-apache-spark-part-2-python-version-da30352c0ca9
- aw: spark stable csv reader automagically detecting encoding
  - https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.encode.html
  - encoding detection examples should be present in pandas csv reader, data.table or similar; trick is probably to have big enough sample size but note performance implications
- aw: databricks ip access list management
  - probably we first need to get existing one and add any new IPs but maybe there is a different way
  - programmatically it's probably better to use terraform but for ad-hoc changes, it may be useful
  - https://docs.databricks.com/api-explorer/workspace/ipaccesslists/update
- aw: json handling
  - https://stackoverflow.com/questions/12472338/flattening-a-list-recursively/12472564#12472564
- aw: create warehouse-like setup from (table-like) config file similar to clean mapping
  - db
    - name, location
  - table
    - db, name, comment
  - column
    - db, table, name, type, format, comment, nullable, ...
- aw: file system get directories and files
  - Getting A List of Folders and Delta Tables in the Fabric Lakehouse: https://fabric.guru/getting-a-list-of-folders-and-delta-tables-in-the-fabric-lakehouse
- aw: delta table cleanup
  - given table location and retention period, vacuum delta table
- aw: function for Rename and drop columns with Delta Lake column mapping
  - https://learn.microsoft.com/en-us/azure/databricks/delta/delta-column-mapping
- aw: delta table file size distribution
- aw: purge delta log files
- 