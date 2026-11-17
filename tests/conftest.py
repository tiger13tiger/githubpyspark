"""共享测试基建。

默认跑 exercises/ 下你自己的实现（一开始全红）。
想看参考答案是否通过：  SOLUTIONS=1 pytest -q
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

from taxilake import bronze, silver
from taxilake.session import SEED_DIR, get_spark

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def impl(module: str):
    """按 SOLUTIONS 环境变量决定导入练习实现还是参考答案。"""
    pkg = "solutions" if os.environ.get("SOLUTIONS") == "1" else "exercises"
    return importlib.import_module(f"{pkg}.{module}")


@pytest.fixture(scope="session")
def spark():
    """整个测试会话共用一个 SparkSession —— 每个测试新建会慢到不可用。"""
    s = get_spark("taxilake-tests", shuffle_partitions=4)
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def trips_csv() -> str:
    return str(SEED_DIR / "trips.csv")


@pytest.fixture(scope="session")
def events_jsonl() -> str:
    return str(SEED_DIR / "events.jsonl")


@pytest.fixture(scope="session")
def raw_trips(spark):
    return bronze.read_trips(spark)


@pytest.fixture(scope="session")
def zones(spark):
    return bronze.read_zones(spark).select("location_id", "borough", "zone")


@pytest.fixture(scope="session")
def clean_trips(spark, raw_trips):
    """给练习 02/03/04 用的干净数据（含 pickup_ts），已 cache。"""
    return silver.clean_trips(raw_trips).cache()
