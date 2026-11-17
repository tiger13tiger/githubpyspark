"""练习 05 —— 半结构化 JSON（GH Archive 风格）

要点：真实数据很少是扁平的。嵌套 struct 用点号访问，
数组用 explode 炸开。data/seed/events.jsonl 里是嵌套事件。
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def load_events(spark: SparkSession, path: str) -> DataFrame:
    """读 JSON Lines 文件。这里允许用推断 schema（练习方便）。"""
    raise NotImplementedError


def flatten_events(df: DataFrame) -> DataFrame:
    """把嵌套结构拍平成列：
      event_id, event_type, created_ts(timestamp),
      actor_login, repo_name, repo_language
    """
    raise NotImplementedError


def commits_per_repo(df: DataFrame) -> DataFrame:
    """PushEvent 里 payload.commits 是数组。统计每个仓库的 commit 总数。
    输出列：repo_name, commits，按 commits 降序。

    提示：先 filter 出 PushEvent，再 explode 数组。
    """
    raise NotImplementedError


def top_language_by_actor(df: DataFrame) -> DataFrame:
    """每个 actor 活动最多的编程语言。
    输出列：actor_login, repo_language, events。
    每个 actor 只保留一行（并列时取语言名字母序靠前的）。

    这题是窗口 + 嵌套的组合，有点难度。
    """
    raise NotImplementedError
