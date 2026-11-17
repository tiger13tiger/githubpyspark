"""练习 02 参考答案。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def trips_per_day(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_date", F.to_date("pickup_ts"))
          .groupBy("pickup_date")
          .agg(F.count("*").alias("trips"))
          .orderBy("pickup_date")
    )


def zone_stats(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pu_location_id")
          .agg(
              F.count("*").alias("trips"),
              F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
              F.max("trip_distance").alias("max_distance"),
              F.countDistinct("vendor_id").alias("distinct_vendors"),
          )
          .orderBy(F.col("trips").desc())
    )


def payment_mix(df: DataFrame) -> DataFrame:
    # 空 partitionBy 的窗口 = 整个数据集一个窗口，可以在聚合结果上拿到总数，
    # 比 crossJoin 一个单行表更简洁（代价是所有数据汇到一个分区，
    # 仅适用于聚合后的小结果集）。
    total = Window.partitionBy()
    return (
        df.groupBy("payment_type")
          .agg(F.count("*").alias("trips"))
          .withColumn("share", F.round(F.col("trips") / F.sum("trips").over(total), 4))
          .orderBy(F.col("trips").desc())
    )
