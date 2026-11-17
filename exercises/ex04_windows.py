"""练习 04 —— 窗口函数

要点：窗口函数在"分组"的同时保留明细行，面试高频考点。
row_number / rank / dense_rank 的区别一定要讲得清。
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def rank_zones_by_day(df: DataFrame) -> DataFrame:
    """每天各上车点的接单量排名。
    输出列：pickup_date, pu_location_id, trips, rank(每天从1开始)。

    并列时名次不跳号（1,2,2,3）—— 想清楚该用哪个排名函数。
    """
    raise NotImplementedError


def running_revenue(df: DataFrame) -> DataFrame:
    """按日期累计营收。
    输出列：pickup_date, daily_revenue(保留2位), cumulative_revenue(保留2位)。

    提示：窗口加 .rowsBetween(Window.unboundedPreceding, Window.currentRow)。
    """
    raise NotImplementedError


def fare_vs_zone_median(df: DataFrame) -> DataFrame:
    """每笔行程的票价与它所在上车点中位数票价的差值。
    输出：原有列 + zone_median_fare + fare_delta(= fare_amount - 中位数)。

    提示：percentile_approx 可以用在窗口里。
    """
    raise NotImplementedError
