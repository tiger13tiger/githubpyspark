"""练习 05 参考答案。"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def load_events(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.json(path)


def flatten_events(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_timestamp("created_at").alias("created_ts"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.name").alias("repo_name"),
        F.col("repo.language").alias("repo_language"),
    )


def commits_per_repo(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("type") == "PushEvent")
          # explode 丢弃空数组所在的行；想保留就用 explode_outer
          .select(F.col("repo.name").alias("repo_name"),
                  F.explode("payload.commits").alias("commit"))
          .groupBy("repo_name")
          .agg(F.count("*").alias("commits"))
          .orderBy(F.col("commits").desc())
    )


def top_language_by_actor(df: DataFrame) -> DataFrame:
    counts = (
        df.select(F.col("actor.login").alias("actor_login"),
                  F.col("repo.language").alias("repo_language"))
          .groupBy("actor_login", "repo_language")
          .agg(F.count("*").alias("events"))
    )
    # 排序键里带上语言名，让并列情况下的结果确定（测试才可复现）
    w = Window.partitionBy("actor_login").orderBy(
        F.col("events").desc(), F.col("repo_language").asc()
    )
    return (
        counts.withColumn("rn", F.row_number().over(w))
              .filter(F.col("rn") == 1)
              .drop("rn")
              .orderBy("actor_login")
    )
