"""端到端流水线：bronze -> silver -> gold，产物写成分区 Parquet。"""
from __future__ import annotations

from pyspark.sql import functions as F

from . import bronze, gold, silver
from .session import LAKE_DIR, get_spark


def main() -> None:
    spark = get_spark("taxilake-pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    raw_trips = bronze.read_trips(spark)
    weather = bronze.read_weather(spark)
    zones = bronze.read_zones(spark)
    print(f"bronze: {raw_trips.count()} 行原始行程")

    clean = silver.clean_trips(raw_trips)
    enriched = silver.join_weather(clean, weather).cache()
    print(f"silver: {enriched.count()} 行（清洗掉 {raw_trips.count() - enriched.count()} 行）")

    # 按日期分区写出 —— 下游按天过滤时可以做分区裁剪
    (enriched.write.mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(str(LAKE_DIR / "silver" / "trips")))

    demand = gold.hourly_demand(enriched, zones)
    effect = gold.rain_effect(enriched)
    demand.write.mode("overwrite").parquet(str(LAKE_DIR / "gold" / "hourly_demand"))
    effect.write.mode("overwrite").parquet(str(LAKE_DIR / "gold" / "rain_effect"))

    print("\n各区高峰时段 Top 3：")
    demand.filter(F.col("demand_rank") <= 3).orderBy("borough", "demand_rank").show(truncate=False)
    print("雨天效应：")
    effect.show(truncate=False)
    print(f"产物已写入 {LAKE_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
