from pyspark.sql import functions as F

from tests.conftest import impl

ex = impl("ex02_aggregations")


def test_trips_per_day(clean_trips):
    out = ex.trips_per_day(clean_trips)
    assert set(out.columns) == {"pickup_date", "trips"}
    rows = out.collect()
    assert len(rows) == 31, "种子数据覆盖 2024 年 3 月整月"
    dates = [r["pickup_date"] for r in rows]
    assert dates == sorted(dates)
    assert sum(r["trips"] for r in rows) == clean_trips.count()


def test_zone_stats(clean_trips):
    out = ex.zone_stats(clean_trips)
    assert set(out.columns) == {
        "pu_location_id", "trips", "avg_fare", "max_distance", "distinct_vendors",
    }
    rows = out.collect()
    assert len(rows) == 6
    counts = [r["trips"] for r in rows]
    assert counts == sorted(counts, reverse=True), "必须按 trips 降序"
    assert all(r["distinct_vendors"] <= 2 for r in rows)


def test_payment_mix(clean_trips):
    out = ex.payment_mix(clean_trips)
    assert set(out.columns) == {"payment_type", "trips", "share"}
    shares = [r["share"] for r in out.collect()]
    assert abs(sum(shares) - 1.0) < 1e-3, f"占比之和应为 1，实际 {sum(shares)}"
