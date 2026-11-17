"""练习 01 —— 读取、schema、选择、过滤、派生列

跑测试：  make test-ex N=01
看答案：  solutions/ex01_basics.py（卡住超过 15 分钟再看）

要点：PySpark 是惰性的。所有 select/filter/withColumn 只是在搭一张
执行计划图，直到 count()/collect()/write 这类 action 才真正算。
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def load_trips(spark: SparkSession, path: str) -> DataFrame:
    """用显式 schema 读 CSV（不要用 inferSchema），表头为第一行。

    提示：taxilake.schemas.TRIPS_SCHEMA 已经写好了。
    """
    raise NotImplementedError


def add_trip_minutes(df: DataFrame) -> DataFrame:
    """新增 double 类型列 trip_minutes = 下车时间 - 上车时间（分钟）。

    原始的两个时间列是字符串，需要先 to_timestamp。
    提示：timestamp cast 成 long 得到秒数。
    """
    raise NotImplementedError


def valid_trips(df: DataFrame) -> DataFrame:
    """只保留合理的行程：
      - fare_amount > 0
      - passenger_count 非空且 > 0
      - 0 < trip_distance < 100
    """
    raise NotImplementedError


def cheapest_n(df: DataFrame, n: int) -> list[float]:
    """返回最便宜的 n 笔行程的 total_amount，升序。

    思考：这里用 orderBy().limit().collect() 对比
    orderBy().collect()[:n]，执行计划差在哪？用 .explain() 看看。
    """
    raise NotImplementedError
