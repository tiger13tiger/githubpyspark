# githubpyspark — PySpark 练习 + NYC 出租车数据湖仓

一个**零成本**的 PySpark 练习项目：Spark 开源免费、数据源公开免费、CI 用 GitHub Actions 免费额度、
本地 `local[*]` 模式就能跑，不需要集群、不需要信用卡、不需要任何 API Key。

练法是**测试驱动**的：`exercises/` 里每个函数都是待填的 `TODO`，
`tests/` 里有对应的断言。你写代码直到测试从红变绿，卡住了再看 `solutions/`。

---

## 30 秒上手

```bash
make setup
```

```bash
make test
```

第一次跑会看到 **21 个失败 + 5 个通过**。这是对的 —— 失败的是等你实现的练习，
通过的 5 个是 `src/taxilake/` 参考实现的回归测试。

```bash
make test-ex N=01
```

只跑第 1 关。然后打开 `exercises/ex01_basics.py`，把 `raise NotImplementedError` 换成你的实现。

验证参考答案全绿：

```bash
SOLUTIONS=1 .venv/bin/pytest -q
```

---

## 练习路线（建议顺序，每关 1–2 小时）

| 关 | 文件 | 练什么 | 关键概念 |
|---|---|---|---|
| 01 | `exercises/ex01_basics.py` | 读 CSV、显式 schema、select/filter/withColumn | 惰性求值、transformation vs action、为什么别用 `inferSchema` |
| 02 | `exercises/ex02_aggregations.py` | groupBy + 多聚合、占比计算 | shuffle、`spark.sql.shuffle.partitions`、空窗口技巧 |
| 03 | `exercises/ex03_joins.py` | left/anti join、同表 join 两次 | **broadcast join vs sort-merge join**、列名冲突 |
| 04 | `exercises/ex04_windows.py` | 排名、累计、组内中位数 | `row_number`/`rank`/`dense_rank` 的区别、`rowsBetween` |
| 05 | `exercises/ex05_nested.py` | 嵌套 JSON 展平、数组 explode | struct 点号访问、`explode` vs `explode_outer` |
| 06 | `exercises/ex06_performance.py` | 分区写出、分区裁剪、重分区 | 读 `.explain()`、`PartitionFilters`、`repartition` vs `coalesce` |

练完 6 关，再去读 `src/taxilake/`（bronze → silver → gold 的完整流水线），
那是"真实项目该长什么样"的参考，然后跑：

```bash
make pipeline
```

### 进阶（练完基础再做）
1. **换成真实数据**：`python scripts/download_tlc.py 2024-01 2024-02 2024-03`，
   然后把 `bronze.py` 改成读 Parquet。真实数据有 300 万行/月，你会第一次真正感受到
   分区和 broadcast 的意义。
2. **加 Delta Lake**：`pip install delta-spark`，把 silver 层改成 Delta 表，
   练 `MERGE INTO`（增量更新）和 time travel。
3. **加 Structured Streaming**：把 Parquet 目录当流源（`readStream.parquet`），
   练 watermark 和窗口聚合。
4. **加 MLlib**：用 silver 数据训一个小费率预测模型，练 `Pipeline` / `VectorAssembler`。
5. **自己设计一关 07**：想一个业务问题，先写测试，再写实现。能独立出题说明真会了。

---

## 免费数据源清单

| 数据源 | 本项目用法 | 获取方式 |
|---|---|---|
| NYC TLC 出租车行程 | 主事实表 | `scripts/download_tlc.py`（公开 Parquet，无 Key） |
| NOAA 天气 | join 的小维表 | 样本数据已模拟；真实数据见 AWS Open Data 的 `noaa-ghcn-pds` |
| GH Archive 事件 | 嵌套 JSON 练习 | 样本已模拟；真实数据 `gharchive.org`（每小时 JSON.gz） |
| 其他可选 | MovieLens 25M、Backblaze 硬盘统计、Wikipedia Pageviews、Stack Exchange 转储 | 均为公开直接下载 |

`data/seed/` 里的样本数据由 `scripts/gen_seed_data.py` 用**固定随机种子**生成，
所以完全可复现，而且故意掺了脏数据（负票价、0 乘客、离群距离、重复行、零时长），
让 silver 层的清洗练习有真实意义。离线、受限网络、CI 里都能跑。

---

## 目录结构

```
├── exercises/        ← 你在这里写代码（带 TODO）
├── solutions/        ← 参考答案（卡住了再看）
├── tests/            ← 断言你的实现是否正确
├── src/taxilake/     ← 完整参考项目：bronze/silver/gold 分层流水线
│   ├── session.py    SparkSession 工厂
│   ├── schemas.py    显式 schema 定义
│   ├── bronze.py     原样落地 + 审计列
│   ├── silver.py     清洗、去重、派生列、join 天气
│   ├── gold.py       分析用聚合表
│   └── pipeline.py   端到端编排
├── scripts/
│   ├── gen_seed_data.py   生成本地样本数据（离线可用）
│   └── download_tlc.py    下载真实 TLC 数据（可选）
└── .github/workflows/ci.yml
```

## 环境说明

已在 **Java 21 + Python 3.13 + PySpark 4.3.0** 下验证通过。

PySpark 在新版 setuptools 上装不动（`AttributeError: install_layout`），
所以 `make setup` 里固定了 `setuptools<81` 并加了 `--no-build-isolation`。

## 常用命令

| 命令 | 作用 |
|---|---|
| `make setup` | 建虚拟环境、装依赖 |
| `make seed` | 重新生成样本数据 |
| `make test` | 跑全部测试 |
| `make test-ex N=03` | 只跑第 3 关 |
| `make pipeline` | 跑完整流水线并打印结果 |
| `make clean` | 清掉数据湖产物 |
