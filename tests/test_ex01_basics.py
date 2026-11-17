from pyspark.sql import functions as F

from tests.conftest import impl

ex = impl("ex01_basics")


def test_load_trips_uses_explicit_schema(spark, trips_csv):
    df = ex.load_trips(spark, trips_csv)
    assert df.count() == 4040, "表头行不该被当成数据"
    assert dict(df.dtypes)["fare_amount"] == "double"
    assert dict(df.dtypes)["passenger_count"] == "int"


def test_add_trip_minutes(spark, raw_trips):
    df = ex.add_trip_minutes(raw_trips)
    assert "trip_minutes" in df.columns
    assert dict(df.dtypes)["trip_minutes"] == "double"
    # 种子数据里每 100 行有一笔零时长行程
    assert df.filter(F.col("trip_minutes") == 0).count() > 0
    assert df.filter(F.col("trip_minutes") < 0).count() == 0


def test_valid_trips_drops_bad_rows(spark, raw_trips):
    out = ex.valid_trips(raw_trips)
    assert out.count() < raw_trips.count()
    assert out.filter(F.col("fare_amount") <= 0).count() == 0
    assert out.filter(F.col("passenger_count").isNull()).count() == 0
    assert out.filter(F.col("passenger_count") <= 0).count() == 0
    assert out.filter(F.col("trip_distance") <= 0).count() == 0
    assert out.filter(F.col("trip_distance") >= 100).count() == 0


def test_cheapest_n(spark, raw_trips):
    got = ex.cheapest_n(ex.valid_trips(raw_trips), 5)
    assert len(got) == 5
    assert got == sorted(got), "必须按 total_amount 升序"
