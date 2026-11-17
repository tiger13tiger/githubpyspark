"""参考实现（src/taxilake）的回归测试 —— 这些一开始就是绿的。

作用：给你一份『测试长什么样』的范例，也保证你改 src 时不会悄悄改坏。
"""
from pyspark.sql import functions as F

from taxilake import gold, silver


def test_clean_trips_removes_dirty_rows(raw_trips):
    out = silver.clean_trips(raw_trips)
    assert out.count() < raw_trips.count()
    for col, bad in [("fare_amount", F.col("fare_amount") <= 0),
                     ("passenger_count", F.col("passenger_count") <= 0),
                     ("trip_minutes", F.col("trip_minutes") <= 0)]:
        assert out.filter(bad).count() == 0, f"{col} 仍有脏数据"


def test_clean_trips_is_deduplicated(raw_trips):
    out = silver.clean_trips(raw_trips)
    keys = ["vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pu_location_id", "do_location_id", "total_amount"]
    assert out.select(*keys).distinct().count() == out.count()


def test_join_weather_keeps_row_count(spark, raw_trips):
    from taxilake import bronze
    clean = silver.clean_trips(raw_trips)
    out = silver.join_weather(clean, bronze.read_weather(spark))
    assert out.count() == clean.count(), "left join 天气不该改变行数"
    assert out.filter(F.col("is_rainy").isNull()).count() == 0


def test_hourly_demand_ranks_within_borough(spark, raw_trips, zones):
    from taxilake import bronze
    clean = silver.join_weather(silver.clean_trips(raw_trips), bronze.read_weather(spark))
    out = gold.hourly_demand(clean, zones)
    firsts = out.filter(F.col("demand_rank") == 1)
    assert firsts.count() == out.select("borough").distinct().count()


def test_rain_effect_covers_both_cases(spark, raw_trips):
    from taxilake import bronze
    clean = silver.join_weather(silver.clean_trips(raw_trips), bronze.read_weather(spark))
    out = gold.rain_effect(clean).collect()
    assert {r["is_rainy"] for r in out} == {True, False}
