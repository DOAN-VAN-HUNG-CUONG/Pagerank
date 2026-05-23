# =============================================================================
# PageRank — Big Data Project   |   convenience targets
# =============================================================================
# Usage:  make help
# -----------------------------------------------------------------------------

PYTHON    ?= python3
PIP       ?= pip3
INPUT     ?= data/graph.txt
ITERS     ?= 20
PAPER_DIR ?= paper

.DEFAULT_GOAL := help

IMAGE ?= pagerank:latest

.PHONY: help install install-dev test lint format \
        run-core run-mrjob run-spark run-streaming \
        benchmark benchmark-full figures gen-graph java-build \
        paper paper-preview \
        docker-build docker-test docker-run clean

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

benchmark:           ## Quick benchmark (small sizes, 3 runs) -> output/benchmark_results.csv
	$(PYTHON) tools/benchmark.py --generate --sizes 1000 5000 10000 \
	    --repeat 3 --iterations $(ITERS)

benchmark-full:      ## Full benchmark incl. PySpark (one command), then regenerate figures
	bash scripts/run_full_benchmark.sh

figures:             ## Regenerate the IEEE figures from the benchmark CSV
	$(PYTHON) tools/visualize.py --csv output/benchmark_results.csv --input $(INPUT)

java-build:          ## Build the Java MapReduce jar (requires Maven + Hadoop)
	cd java && mvn -q clean package

paper:               ## Build the IEEE submission PDF (paper/main.pdf; needs IEEEtran.cls)
	cd $(PAPER_DIR) && latexmk -pdf -interaction=nonstopmode main.tex || \
	    (pdflatex -interaction=nonstopmode main.tex && pdflatex -interaction=nonstopmode main.tex)

paper-preview:       ## Build the article-class preview PDF (no IEEEtran.cls required)
	cd $(PAPER_DIR) && latexmk -pdf -interaction=nonstopmode preview.tex || \
	    (pdflatex -interaction=nonstopmode preview.tex && pdflatex -interaction=nonstopmode preview.tex)

docker-build:        ## Build the Docker image (IMAGE=pagerank:latest)
	docker build -t $(IMAGE) .

docker-test:         ## Run the test suite inside the Docker image
	docker run --rm $(IMAGE) pytest -m "not spark" -q

docker-run:          ## Run the reference engine inside the Docker image
	docker run --rm $(IMAGE) python -m core.cli data/graph.txt --iterations $(ITERS)

clean:               ## Remove generated artifacts (keeps committed paper PDFs)
	rm -rf output/*_rdd output/*_df output/pagerank_spark output/init_state.txt \
	       metastore_db derby.log spark-warehouse .pytest_cache \
	       java/target docs/figures/*.png
	rm -f $(PAPER_DIR)/*.aux $(PAPER_DIR)/*.log $(PAPER_DIR)/*.out \
	      $(PAPER_DIR)/*.fls $(PAPER_DIR)/*.fdb_latexmk $(PAPER_DIR)/*.bbl $(PAPER_DIR)/*.blg
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
