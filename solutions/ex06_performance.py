"""练习 06 参考答案。"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


def write_partitioned(df: DataFrame, out_path: str) -> None:
    out = df if "pickup_date" in df.columns else df.withColumn(
        "pickup_date", F.to_date("pickup_ts")
    )
    out.write.mode("overwrite").partitionBy("pickup_date").parquet(out_path)


def read_one_day(spark: SparkSession, path: str, day: str) -> DataFrame:
    # 对分区列做等值过滤 -> Spark 只会列出那一个目录，
    # explain() 里体现为 PartitionFilters: [pickup_date = 2024-03-05]
    return spark.read.parquet(path).filter(F.col("pickup_date") == day)


def repartition_then_count(df: DataFrame, n: int) -> tuple[int, int]:
    wide = df.repartition(n)              # 全量 shuffle，可增可减
    narrow = wide.coalesce(1)             # 只合并分区，不 shuffle
    return wide.rdd.getNumPartitions(), narrow.rdd.getNumPartitions()
