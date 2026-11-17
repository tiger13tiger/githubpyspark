"""统一的 SparkSession 构造入口。

本地练习用 local[*]；真要上集群时只需改 master 和 shuffle 分区数。
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEED_DIR = PROJECT_ROOT / "data" / "seed"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"


def get_spark(app_name: str = "taxilake", shuffle_partitions: int = 8) -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        # 默认 200 个 shuffle 分区对本地小数据是纯浪费，调小能明显加快
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
