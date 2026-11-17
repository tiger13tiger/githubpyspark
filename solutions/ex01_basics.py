"""练习 01 参考答案。"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from taxilake.schemas import TRIPS_SCHEMA


def load_trips(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, schema=TRIPS_SCHEMA, header=True)


def add_trip_minutes(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
          .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
          .withColumn(
              "trip_minutes",
              (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
          )
    )


def valid_trips(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("fare_amount") > 0)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") < 100)
    )


def cheapest_n(df: DataFrame, n: int) -> list[float]:
    # orderBy + limit 会被优化成 TakeOrderedAndProject，只在每个分区取 top-n
    # 再汇总，不需要全排序 —— 比 collect()[:n] 快得多也省内存。
    rows = df.orderBy(F.col("total_amount").asc()).limit(n).collect()
    return [r["total_amount"] for r in rows]
