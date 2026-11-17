"""练习 02 —— 分组聚合

要点：groupBy 会触发 shuffle（数据跨分区搬运），是 Spark 作业最
常见的性能瓶颈。spark.sql.shuffle.partitions 默认 200，小数据上调小。
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def trips_per_day(df: DataFrame) -> DataFrame:
    """按上车日期聚合，输出列：pickup_date(date), trips(long), 按日期升序。

    输入已有 pickup_ts 列。
    """
    raise NotImplementedError


def zone_stats(df: DataFrame) -> DataFrame:
    """按 pu_location_id 聚合，输出列：
      pu_location_id, trips, avg_fare(保留2位), max_distance, distinct_vendors
    按 trips 降序。

    提示：一次 .agg() 里可以放多个聚合函数；去重计数用 countDistinct。
    """
    raise NotImplementedError


def payment_mix(df: DataFrame) -> DataFrame:
    """每种 payment_type 的笔数占总笔数的比例。
    输出列：payment_type, trips, share(保留4位，0~1)。

    思考：要算占比就需要总数。用 crossJoin 一个单行总数表，
    或者用空 partition 的窗口函数 —— 后者更地道。
    """
    raise NotImplementedError
