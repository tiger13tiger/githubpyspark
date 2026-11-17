from pyspark.sql import functions as F

from tests.conftest import impl

ex = impl("ex03_joins")


def test_attach_zone_names(clean_trips, zones):
    out = ex.attach_zone_names(clean_trips, zones)
    assert out.count() == clean_trips.count(), "left join 不该增减行数"
    assert "borough" in out.columns and "zone" in out.columns
    assert out.columns.count("pu_location_id") == 1, "键列不该重复"
    assert out.filter(F.col("borough").isNull()).count() == 0


def test_trips_missing_zone(clean_trips, zones):
    assert ex.trips_missing_zone(clean_trips, zones) == 0
    partial = zones.filter(F.col("location_id") != 5)
    expected = clean_trips.filter(F.col("pu_location_id") == 5).count()
    assert ex.trips_missing_zone(clean_trips, partial) == expected


def test_borough_pairs(clean_trips, zones):
    out = ex.borough_pairs(clean_trips, zones)
    assert set(out.columns) == {"pu_borough", "do_borough", "trips"}
    rows = out.collect()
    assert sum(r["trips"] for r in rows) == clean_trips.count()
    counts = [r["trips"] for r in rows]
    assert counts == sorted(counts, reverse=True)


def test_broadcast_shows_in_plan(clean_trips, zones):
    plan = ex.attach_zone_names(clean_trips, zones)._jdf.queryExecution().executedPlan().toString()
    assert "Broadcast" in plan, "执行计划里应出现 BroadcastHashJoin —— 用 F.broadcast()"
