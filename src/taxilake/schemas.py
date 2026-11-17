"""显式 schema —— 生产里永远不要对大数据集用 inferSchema。

inferSchema 会额外全量扫一遍文件，而且类型可能随数据漂移。
"""
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

TRIPS_SCHEMA = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pu_location_id", IntegerType(), True),
    StructField("do_location_id", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

WEATHER_SCHEMA = StructType([
    StructField("obs_hour", StringType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("precip_mm", DoubleType(), True),
    StructField("wind_kph", DoubleType(), True),
])

ZONES_SCHEMA = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("borough", StringType(), True),
    StructField("zone", StringType(), True),
])
