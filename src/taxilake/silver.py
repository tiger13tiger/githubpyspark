"""Silver 层：清洗、类型转换、去重、派生列、join 天气。"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

# 业务规则集中成常量，而不是散落在代码里的魔法数字
MIN_FARE = 0.0
MAX_TRIP_DISTANCE_MI = 100.0
MAX_TRIP_MINUTES = 8 * 60


def clean_trips(trips: DataFrame) -> DataFrame:
    """把 bronze 行程清成可用的事实数据。

    规则：转时间戳 -> 算时长 -> 过滤不可能的记录 -> 按业务键去重。
    """
    typed = (
        trips
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
    )
    enriched = (
        typed
        .withColumn(
            "trip_minutes",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
        )
        .withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_ts")))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))
        .withColumn(
            "tip_pct",
            F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount"))
             .otherwise(F.lit(None).cast("double")),
        )
    )
    valid = enriched.filter(
        (F.col("fare_amount") > MIN_FARE)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") < MAX_TRIP_DISTANCE_MI)
        & (F.col("trip_minutes") > 0)
        & (F.col("trip_minutes") < MAX_TRIP_MINUTES)
    )
    # 业务键去重：同一车商、同一上下车时间、同一起终点视为重复上报
    return valid.dropDuplicates([
        "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "pu_location_id", "do_location_id", "total_amount",
    ])


def join_weather(trips: DataFrame, weather: DataFrame) -> DataFrame:
    """按小时把天气贴到行程上。天气表很小 -> 广播，避免 shuffle。"""
    w = (
        weather
        .select(
            F.to_timestamp("obs_hour").alias("pickup_hour"),
            "temp_c", "precip_mm", "wind_kph",
        )
        .withColumn("is_rainy", F.col("precip_mm") > 0.5)
    )
    return trips.join(F.broadcast(w), on="pickup_hour", how="left")
