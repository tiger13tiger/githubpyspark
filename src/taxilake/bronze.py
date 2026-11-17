"""Bronze 层：原样落地 + 审计列。不做任何业务清洗。"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from .schemas import TRIPS_SCHEMA, WEATHER_SCHEMA, ZONES_SCHEMA
from .session import SEED_DIR


def _with_audit(df: DataFrame, source: str) -> DataFrame:
    """加血缘/审计列 —— 出问题时能回答『这行数据哪来的、什么时候进来的』。"""
    return (
        df.withColumn("_source_file", F.input_file_name())
          .withColumn("_source", F.lit(source))
          .withColumn("_ingested_at", F.current_timestamp())
    )


def read_trips(spark: SparkSession) -> DataFrame:
    df = spark.read.csv(str(SEED_DIR / "trips.csv"), schema=TRIPS_SCHEMA, header=True)
    return _with_audit(df, "seed/trips.csv")


def read_weather(spark: SparkSession) -> DataFrame:
    df = spark.read.csv(str(SEED_DIR / "weather.csv"), schema=WEATHER_SCHEMA, header=True)
    return _with_audit(df, "seed/weather.csv")


def read_zones(spark: SparkSession) -> DataFrame:
    df = spark.read.csv(str(SEED_DIR / "zones.csv"), schema=ZONES_SCHEMA, header=True)
    return _with_audit(df, "seed/zones.csv")
