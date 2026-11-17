"""（可选）下载真实的 NYC TLC 行程数据。

数据源：纽约市出租车和豪华车委员会官方公开数据，Parquet 格式，无需 API Key。
官方页面：https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

用法：
    python scripts/download_tlc.py 2024-01 2024-02 2024-03

注意：
  * 每个月文件约 50MB Parquet（解压后数百 MB），别提交进 git（已在 .gitignore 里）。
  * 真实数据的列名用大驼峰（VendorID、tpep_pickup_datetime...），
    和 data/seed 的小写下划线不同，读取后需要 toDF() 重命名。
  * 如果你在受限网络（比如公司代理或云沙箱）里，下载可能被拦，
    这时继续用 data/seed 的样本数据即可，练习内容完全一样。
"""
from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def download(month: str) -> Path:
    name = f"yellow_tripdata_{month}.parquet"
    dest = RAW_DIR / name
    if dest.exists():
        print(f"已存在，跳过：{name}")
        return dest
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    url = f"{BASE}/{name}"
    print(f"下载 {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"  -> {dest}  ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


if __name__ == "__main__":
    months = sys.argv[1:] or ["2024-03"]
    for m in months:
        try:
            download(m)
        except Exception as exc:  # 网络受限是常见情况，不要让脚本整体崩掉
            print(f"下载 {m} 失败：{exc}\n可以继续用 data/seed 的样本数据练习。")
