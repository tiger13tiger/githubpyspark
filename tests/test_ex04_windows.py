from pyspark.sql import functions as F

from tests.conftest import impl

ex = impl("ex04_windows")


def test_rank_zones_by_day(clean_trips):
    out = ex.rank_zones_by_day(clean_trips)
    assert set(out.columns) == {"pickup_date", "pu_location_id", "trips", "rank"}
    per_day = out.groupBy("pickup_date").agg(F.min("rank").alias("lo"))
    assert per_day.filter(F.col("lo") != 1).count() == 0, "每天的名次都要从 1 开始"
    # dense_rank：名次连续、不跳号
    dense = out.groupBy("pickup_date").agg(
        F.max("rank").alias("mx"), F.countDistinct("rank").alias("n")
    )
    assert dense.filter(F.col("mx") != F.col("n")).count() == 0, \
        "并列时名次不该跳号 —— 用 dense_rank 而不是 rank"


def test_running_revenue(clean_trips):
    out = ex.running_revenue(clean_trips)
    assert set(out.columns) == {"pickup_date", "daily_revenue", "cumulative_revenue"}
    rows = out.orderBy("pickup_date").collect()
    cum = [r["cumulative_revenue"] for r in rows]
    assert cum == sorted(cum), "累计值必须单调不减"
    assert abs(cum[-1] - sum(r["daily_revenue"] for r in rows)) < 1.0
    assert abs(cum[0] - rows[0]["daily_revenue"]) < 1e-6


def test_fare_vs_zone_median(clean_trips):
    out = ex.fare_vs_zone_median(clean_trips)
    assert "zone_median_fare" in out.columns and "fare_delta" in out.columns
    assert out.count() == clean_trips.count(), "窗口函数不该减少行数"
    chk = out.withColumn(
        "bad", F.abs(F.col("fare_amount") - F.col("zone_median_fare") - F.col("fare_delta")) > 1e-6
    )
    assert chk.filter("bad").count() == 0
    # 同一上车点内中位数应当唯一
    assert out.select("pu_location_id", "zone_median_fare").distinct().count() == 6
