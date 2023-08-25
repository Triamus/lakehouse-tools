# lakehouse-tools

lakehouse-tools is currently a random collection of functions and ideas targeting workloads around Spark, Delta Lake, Databricks and some general utilities. Current goals are as follows:

- build helpful functions/methods
  - no overall design idea yet
- document ideas for functions (ideas backlog) and discuss those
  - implement what has been agreed to be valuable (actual backlog)
  - dismiss or park what is not of value
- (re-)learn building OSS projects on GitHub end2end
  - collaborative development incl
    - branching strategy
	- issues
	  - issue template
	- bugs
	- PRs
    - pr template
	- design discussions
	- releases
	- etc
  - local dev env best practices
    - package manager
    - env manager
    - runtime
    - docker image
  - CI/CD
    - GitHub Actions
    - packaging
    - DevSecOps
- python package best practices
- docs best practices

## Setup

Local import from root directory as `import lakehouse_tools as lt`.

Here's an example of how you can create a sample spark dataframe:

```python
import lakehouse_tools as lt

lt.create_sample_df(spark).show()

+-----------+---------+------+--------+----------+
|stringIdCol|stringCol|intCol|floatCol|booleanCol|
+-----------+---------+------+--------+----------+
|          1|        a|     1|     1.0|      true|
|          2|        b|     2|     2.0|     false|
|          3|        c|     3|     3.0|      true|
+-----------+---------+------+--------+----------+
```

## Package inspiration and ideas

- [mack: Delta Lake helper methods in PySpark](https://github.com/MrPowers/mack)
- [levi: Delta Lake helper methods. No Spark dependency.](https://github.com/MrPowers/levi)
- [discoverx: Scans the Databricks Lakehouse and provides semantic classification of columns, along with cross-table query based on class.](https://github.com/databrickslabs/discoverx)
- [dbldatagen: The Databricks Labs synthetic data generator](https://github.com/databrickslabs/dbldatagen)
- [pandera: A light-weight, flexible, and expressive statistical data testing library](https://github.com/unionai-oss/pandera)
- [pandas on spark](https://github.com/apache/spark/blob/master/python/pyspark/pandas)