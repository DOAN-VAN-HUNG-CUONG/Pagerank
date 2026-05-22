# =============================================================================
# PageRank — Big Data Project   |   convenience targets
# =============================================================================
# Usage:  make help
# -----------------------------------------------------------------------------

PYTHON ?= python3
PIP    ?= pip3
INPUT  ?= data/graph.txt
ITERS  ?= 20

.DEFAULT_GOAL := help

.PHONY: help install install-dev test lint format \
        run-core run-mrjob run-spark run-streaming \
        benchmark figures gen-graph java-build clean

help:                ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install:             ## Install runtime + tooling dependencies
	$(PIP) install -r requirements.txt

install-dev:         ## Install dev dependencies (pytest, pyspark, mrjob, ruff)
	$(PIP) install -r requirements-dev.txt

test:                ## Run the test suite
	$(PYTHON) -m pytest

lint:                ## Lint with ruff
	ruff check python tools tests

format:              ## Auto-format with ruff
	ruff format python tools tests

run-core:            ## Run the pure-Python reference engine
	PYTHONPATH=python $(PYTHON) -m core.cli $(INPUT) --iterations $(ITERS)

run-mrjob:           ## Run the mrjob local implementation
	$(PYTHON) python/mrjob/pagerank_mrjob.py $(INPUT) --iterations $(ITERS) \
	    --output output/pagerank_mrjob.txt

run-spark:           ## Run the PySpark implementation (RDD + DataFrame)
	spark-submit --master 'local[*]' python/pyspark/pagerank_spark.py \
	    --input $(INPUT) --mode both --iterations $(ITERS)

run-streaming:       ## Simulate the Hadoop Streaming pipeline locally
	./scripts/run_streaming.sh $(ITERS) --local $(INPUT)

gen-graph:           ## Generate a synthetic scale-free graph (NODES=, OUT=)
	$(PYTHON) tools/generate_graph.py --nodes $(or $(NODES),1000) \
	    --output $(or $(OUT),data/graph_large.txt)

benchmark:           ## Benchmark implementations and write a JSON report
	$(PYTHON) tools/benchmark.py --input $(INPUT) --iterations $(ITERS)

figures:             ## Generate convergence / comparison / graph figures
	$(PYTHON) tools/visualize.py --input $(INPUT) --iterations $(ITERS)

java-build:          ## Build the Java MapReduce jar (requires Maven + Hadoop)
	cd java && mvn -q clean package

clean:               ## Remove generated artifacts
	rm -rf output/*_rdd output/*_df output/pagerank_spark output/init_state.txt \
	       metastore_db derby.log spark-warehouse .pytest_cache \
	       java/target docs/figures/*.png
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
