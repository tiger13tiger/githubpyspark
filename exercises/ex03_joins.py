"""练习 03 —— join 与广播

要点：大表 join 小表时，broadcast 把小表发到每个 executor，
彻底避免 shuffle。默认阈值 spark.sql.autoBroadcastJoinThreshold=10MB。
用 .explain() 确认计划里是 BroadcastHashJoin 而不是 SortMergeJoin。
"""
from __future__ import annotations

from pyspark.sql import DataFrame


def attach_zone_names(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """给行程贴上上车点的 borough 和 zone 名称。

    zones 的键叫 location_id，trips 的叫 pu_location_id。
    用 left join（保证不丢行程），并显式广播 zones。
    结果不要出现重复的 location_id 列。
    """
    raise NotImplementedError


def trips_missing_zone(trips: DataFrame, zones: DataFrame) -> int:
    """有多少行程的上车点在 zones 维表里查不到？

    提示：left_anti join 就是为这个场景生的。
    """
    raise NotImplementedError


def borough_pairs(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """起点区 -> 终点区 的流量矩阵。
    输出列：pu_borough, do_borough, trips，按 trips 降序。

    注意：同一张维表要 join 两次（上车点和下车点），
    必须用 alias 区分，否则列名冲突。
    """
    raise NotImplementedError
