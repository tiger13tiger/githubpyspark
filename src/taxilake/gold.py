"""Gold 层：面向分析的聚合表。"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def hourly_demand(silver: DataFrame, zones: DataFrame) -> DataFrame:
    """每个行政区每小时的需求与均价，并算出该区内的小时排名。"""
    z = zones.select(F.col("location_id").alias("pu_location_id"), "borough")
    joined = silver.join(F.broadcast(z), on="pu_location_id", how="left")

    agg = (
        joined.groupBy("borough", "pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"),
            F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"),
            F.max("is_rainy").alias("is_rainy"),
        )
    )
    w = Window.partitionBy("borough").orderBy(F.col("trips").desc())
    return agg.withColumn("demand_rank", F.row_number().over(w))


def rain_effect(silver: DataFrame) -> DataFrame:
    """下雨 vs 不下雨：单均价、单均时长、小费率的差异。"""
    return (
        silver.groupBy("is_rainy")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"),
            F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"),
        )
        .orderBy("is_rainy")
    )
