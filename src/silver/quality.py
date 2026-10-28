"""
Data-quality bookkeeping for the Silver layer.

The issue log (`silver/dq_issues`) gets one row per problem Silver finds, with what it did:
    rejected  the row, value or file is not used (rejected rows are the dead-letter queue)
    repaired  the value was rebuilt from other fields of the same record
    removed   a duplicate copy was dropped
    excluded  a whole file was left out on purpose (empty or synthetic test data)

Cleaning code marks problems per row as an array of issue structs (`collect_issues`), and
`issues_to_log` turns them into log rows. Nothing is dropped without a matching log row.
"""

from typing import Optional, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ACTION_REJECTED = "rejected"
ACTION_REPAIRED = "repaired"
ACTION_REMOVED = "removed"
ACTION_EXCLUDED = "excluded"

# The original record is kept with each issue, shortened so a bad file can't bloat the log.
RAW_VALUE_MAX_CHARS = 1000

ISSUE_COLUMNS = ["source", "issue", "action", "source_file", "record_index", "field", "raw_value"]


def issue_if(
    condition: Column,
    issue: str,
    action: Union[str, Column],
    field: Optional[Union[str, Column]] = None,
) -> Column:
    """An issue struct when `condition` holds, otherwise null."""
    action_col = action if isinstance(action, Column) else F.lit(action)
    field_col = field if isinstance(field, Column) else F.lit(field).cast("string")
    return F.when(
        condition,
        F.struct(F.lit(issue).alias("issue"), action_col.alias("action"), field_col.alias("field")),
    )


def collect_issues(*issues: Column) -> Column:
    """Array of the issues that apply to a row (nulls dropped)."""
    return F.filter(F.array(*issues), lambda i: i.isNotNull())


def issues_to_log(rows: DataFrame, source: str) -> DataFrame:
    """Issue-log rows from a DataFrame with `issues`, `source_file`, `record_index`, `raw_value`."""
    return rows.select(
        F.lit(source).alias("source"),
        "source_file",
        "record_index",
        "raw_value",
        F.explode("issues").alias("i"),
    ).select(
        "source",
        F.col("i.issue").alias("issue"),
        F.col("i.action").alias("action"),
        "source_file",
        "record_index",
        F.col("i.field").alias("field"),
        "raw_value",
    )


def raw_json(record: Column) -> Column:
    """The original record as (shortened) JSON for the issue log."""
    return F.substring(F.to_json(record), 1, RAW_VALUE_MAX_CHARS)
