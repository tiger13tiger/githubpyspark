"""生成可复现的本地样本数据，模拟真实数据源的 schema 和脏数据特征。

不需要联网 —— 练习和 CI 都用这份数据。想换成真实数据时，
跑 scripts/download_tlc.py，schema 完全兼容。
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED_DIR = Path(__file__).resolve().parents[1] / "data" / "seed"
RNG = random.Random(42)  # 固定种子 -> 每次生成完全一样，测试才能稳定

ZONES = {
    1: ("Manhattan", "Midtown"),
    2: ("Manhattan", "Upper East Side"),
    3: ("Brooklyn", "Williamsburg"),
    4: ("Queens", "Astoria"),
    5: ("Queens", "JFK Airport"),
    6: ("Bronx", "Fordham"),
}
VENDORS = [1, 2]
PAYMENT = [1, 1, 1, 2, 2, 3, 4]  # 1=信用卡 2=现金 3=免费 4=争议


def gen_trips(n: int = 4000):
    """模拟 NYC TLC yellow_tripdata 的核心字段，并故意掺入脏数据。"""
    start = datetime(2024, 3, 1)
    rows = []
    for i in range(n):
        pickup = start + timedelta(minutes=RNG.randint(0, 31 * 24 * 60))
        duration_min = max(1, int(RNG.gauss(14, 8)))
        pu, do = RNG.choice(list(ZONES)), RNG.choice(list(ZONES))
        distance = round(abs(RNG.gauss(2.8, 2.0)) + 0.1, 2)
        fare = round(3.0 + distance * 2.8 + duration_min * 0.35, 2)
        payment = RNG.choice(PAYMENT)
        tip = round(fare * RNG.uniform(0.0, 0.3), 2) if payment == 1 else 0.0

        row = {
            "vendor_id": RNG.choice(VENDORS),
            "tpep_pickup_datetime": pickup.strftime("%Y-%m-%d %H:%M:%S"),
            "tpep_dropoff_datetime": (pickup + timedelta(minutes=duration_min)).strftime("%Y-%m-%d %H:%M:%S"),
            "passenger_count": RNG.choice([1, 1, 1, 2, 2, 3, 4, 5, 6]),
            "trip_distance": distance,
            "pu_location_id": pu,
            "do_location_id": do,
            "payment_type": payment,
            "fare_amount": fare,
            "tip_amount": tip,
            "total_amount": round(fare + tip + 1.0, 2),
        }

        # ---- 故意植入的脏数据，silver 层要负责清掉 ----
        bucket = i % 100
        if bucket == 3:
            row["fare_amount"] = -row["fare_amount"]          # 负票价（退单）
        elif bucket == 7:
            row["passenger_count"] = 0                        # 0 乘客
        elif bucket == 11:
            row["trip_distance"] = 0.0                        # 0 距离
        elif bucket == 17:
            row["trip_distance"] = 999.9                       # 离群值
        elif bucket == 23:
            row["passenger_count"] = ""                        # 缺失值
        elif bucket == 29:
            row["tpep_dropoff_datetime"] = row["tpep_pickup_datetime"]  # 零时长
        elif bucket == 31:
            rows.append(dict(row))                             # 完全重复行
        rows.append(row)
    return rows


def gen_weather():
    """模拟 NOAA GHCN 的逐小时天气，作为 join 的小维表。"""
    rows = []
    t = datetime(2024, 3, 1)
    while t < datetime(2024, 4, 1):
        is_rain = RNG.random() < 0.18
        rows.append({
            "obs_hour": t.strftime("%Y-%m-%d %H:00:00"),
            "temp_c": round(RNG.gauss(8, 6), 1),
            "precip_mm": round(RNG.uniform(0.2, 12.0), 1) if is_rain else 0.0,
            "wind_kph": round(abs(RNG.gauss(14, 7)), 1),
        })
        t += timedelta(hours=1)
    return rows


def gen_zones():
    return [{"location_id": k, "borough": v[0], "zone": v[1]} for k, v in ZONES.items()]


def gen_events(n: int = 600):
    """模拟 GH Archive 风格的嵌套 JSON —— 练习 explode / 结构体展平。"""
    types = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent"]
    langs = ["Python", "Scala", "Java", "Rust", "Go"]
    out = []
    for i in range(n):
        et = RNG.choice(types)
        payload = {"action": RNG.choice(["opened", "closed", "created"])}
        if et == "PushEvent":
            payload = {"commits": [
                {"sha": f"{i:06x}{j}", "message": f"fix thing {j}", "distinct": True}
                for j in range(RNG.randint(1, 4))
            ]}
        out.append({
            "id": str(10_000_000 + i),
            "type": et,
            "created_at": (datetime(2024, 3, 1) + timedelta(minutes=i * 7)).isoformat() + "Z",
            "actor": {"id": RNG.randint(1, 50), "login": f"dev{RNG.randint(1, 50)}"},
            "repo": {"id": RNG.randint(1, 20), "name": f"org/proj{RNG.randint(1, 20)}",
                     "language": RNG.choice(langs)},
            "payload": payload,
        })
    return out


def write_csv(path: Path, rows: list[dict]):
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  {path.relative_to(path.parents[2])}  ({len(rows)} 行)")


def main():
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    print("生成样本数据 ->")
    write_csv(SEED_DIR / "trips.csv", gen_trips())
    write_csv(SEED_DIR / "weather.csv", gen_weather())
    write_csv(SEED_DIR / "zones.csv", gen_zones())
    path = SEED_DIR / "events.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        events = gen_events()
        for e in events:
            fh.write(json.dumps(e) + "\n")
    print(f"  data/seed/events.jsonl  ({len(events)} 行)")


if __name__ == "__main__":
    main()
