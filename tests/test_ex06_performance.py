from pathlib import Path

from tests.conftest import impl

ex = impl("ex06_performance")


def test_write_partitioned_creates_dirs(clean_trips, tmp_path):
    out = str(tmp_path / "trips")
    ex.write_partitioned(clean_trips, out)
    parts = sorted(p.name for p in Path(out).iterdir() if p.is_dir())
    assert len(parts) == 31, f"应有 31 个日期分区目录，实际 {len(parts)}"
    assert all(p.startswith("pickup_date=") for p in parts)


def test_read_one_day_prunes_partitions(spark, clean_trips, tmp_path):
    out = str(tmp_path / "trips")
    ex.write_partitioned(clean_trips, out)
    df = ex.read_one_day(spark, out, "2024-03-05")
    assert df.count() > 0
    assert df.select("pickup_date").distinct().count() == 1
    plan = df._jdf.queryExecution().executedPlan().toString()
    assert "PartitionFilters: [" in plan and "pickup_date" in plan, \
        "没发生分区裁剪 —— 过滤条件要直接作用在分区列上"


def test_repartition_vs_coalesce(clean_trips):
    wide, narrow = ex.repartition_then_count(clean_trips, 6)
    assert wide == 6
    assert narrow == 1
