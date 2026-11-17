"""练习 06 —— 分区、裁剪与执行计划

这一关没有"标准答案式"的函数，重点是学会读 .explain()。
测试会断言你的写出产物确实做了分区，以及读取时发生了分区裁剪。
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def write_partitioned(df: DataFrame, out_path: str) -> None:
    """按 pickup_date 分区，以 overwrite 模式写成 Parquet。

    写完去看目录结构：data/lake/... 下面是 pickup_date=2024-03-01/ 这种。
    """
    raise NotImplementedError


def read_one_day(spark: SparkSession, path: str, day: str) -> DataFrame:
    """只读某一天的数据（day 形如 "2024-03-05"）。

    关键：过滤条件要能被下推成分区裁剪。
    用 .explain() 看 PartitionFilters 这一行 —— 如果是
    PushedFilters 或者完全没有，说明没裁到。
    """
    raise NotImplementedError


def repartition_then_count(df: DataFrame, n: int) -> tuple[int, int]:
    """返回 (repartition 后的分区数, coalesce 到 1 后的分区数)。

    思考 repartition 和 coalesce 的区别：前者全量 shuffle 可增可减，
    后者只能减少、不 shuffle，但可能导致数据倾斜。
    """
    raise NotImplementedError
