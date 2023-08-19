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
- python package best practices
- docs best practices

## Setup

Local import from root directory as `import lakehouse-tools as lt`.

Here's an example of how you can create a sample spark dataframe:

```python
import lakehouse-tools as lt

lt.create_sample_df(spark).show()

+-----------+---------+------+--------+----------+
|stringIdCol|stringCol|intCol|floatCol|booleanCol|
+-----------+---------+------+--------+----------+
|          1|        a|     1|     1.0|      true|
|          2|        b|     2|     2.0|     false|
|          3|        c|     3|     3.0|      true|
+-----------+---------+------+--------+----------+
```