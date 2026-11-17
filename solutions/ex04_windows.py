"""练习 04 参考答案。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def rank_zones_by_day(df: DataFrame) -> DataFrame:
    daily = (
        df.withColumn("pickup_date", F.to_date("pickup_ts"))
          .groupBy("pickup_date", "pu_location_id")
          .agg(F.count("*").alias("trips"))
    )
    # 并列不跳号 -> dense_rank；rank 会跳号（1,2,2,4）；
    # row_number 强制唯一名次，不体现并列。
    w = Window.partitionBy("pickup_date").orderBy(F.col("trips").desc())
    return daily.withColumn("rank", F.dense_rank().over(w))


def running_revenue(df: DataFrame) -> DataFrame:
    daily = (
        df.withColumn("pickup_date", F.to_date("pickup_ts"))
          .groupBy("pickup_date")
          .agg(F.sum("total_amount").alias("raw_daily"))
    )
    w = Window.orderBy("pickup_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        daily
        .withColumn("cumulative_revenue", F.round(F.sum("raw_daily").over(w), 2))
        .withColumn("daily_revenue", F.round(F.col("raw_daily"), 2))
        .drop("raw_daily")
        .orderBy("pickup_date")
    )


def fare_vs_zone_median(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("pu_location_id")
    return (
        df.withColumn(
            "zone_median_fare",
            F.percentile_approx("fare_amount", 0.5).over(w),
        )
        .withColumn("fare_delta", F.col("fare_amount") - F.col("zone_median_fare"))
    )
