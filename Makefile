VENV := .venv
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip

.PHONY: setup seed test test-ex pipeline clean

setup:                      ## 建虚拟环境并安装依赖
	python3 -m venv $(VENV)
	$(PIP) -q install -U pip "setuptools<81" wheel
	$(PIP) install --no-build-isolation pyspark chispa pytest

seed:                       ## 生成本地样本数据（无需联网）
	$(PY) scripts/gen_seed_data.py

test: seed                  ## 跑全部测试
	$(VENV)/bin/pytest -q

test-ex: seed               ## 只跑某个练习的测试: make test-ex N=01
	$(VENV)/bin/pytest -q tests/test_ex$(N)*.py

pipeline: seed              ## 跑完整 bronze->silver->gold 流水线
	$(PY) -m taxilake.pipeline

clean:
	rm -rf data/lake data/raw .pytest_cache spark-warehouse metastore_db derby.log
