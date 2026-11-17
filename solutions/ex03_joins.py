"""练习 03 参考答案。"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def attach_zone_names(trips: DataFrame, zones: DataFrame) -> DataFrame:
    # 先把维表的键改名成和事实表一致，再用 on="列名" join，
    # 这样结果里只有一份键列，省掉后面 drop 的麻烦。
    z = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough"),
        F.col("zone"),
    )
    return trips.join(F.broadcast(z), on="pu_location_id", how="left")


def trips_missing_zone(trips: DataFrame, zones: DataFrame) -> int:
    z = zones.select(F.col("location_id").alias("pu_location_id"))
    return trips.join(F.broadcast(z), on="pu_location_id", how="left_anti").count()


def borough_pairs(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pu = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pu_borough"),
    )
    do = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("do_borough"),
    )
    return (
        trips.join(F.broadcast(pu), on="pu_location_id", how="inner")
             .join(F.broadcast(do), on="do_location_id", how="inner")
             .groupBy("pu_borough", "do_borough")
             .agg(F.count("*").alias("trips"))
             .orderBy(F.col("trips").desc())
    )
