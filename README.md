# PageRank — Big Data Project

**Python · PySpark · Hadoop Streaming · Java MapReduce · Apache Pig**

---

## 1. Overview

This project implements the **PageRank** algorithm (Brin & Page, 1998) on the **Hadoop**
ecosystem in five different ways, plus a **pure-Python reference engine** (`python/core`)
that acts as a single source of truth. Every distributed implementation is validated
against this engine and independently cross-checked against `networkx.pagerank`,
guaranteeing that the five variants are numerically consistent.

| Implementation | Framework | Runs on |
|---|---|---|
| `core` (reference) | Pure Python | Anywhere (no dependencies) |
| Python (mrjob) | mrjob inline/local/Hadoop | Local / Hadoop |
| Python (PySpark) | PySpark RDD + DataFrame | Spark local / cluster |
| Python (Streaming) | Hadoop Streaming | Hadoop / local simulation |
| Java | Hadoop MapReduce API | Hadoop |
| Pig | Apache Pig Latin | Hadoop (Pig) |

---

## 2. Algorithm

The reference engine uses the general recurrence, correctly handling **dangling nodes**,
**edge weights**, and a **personalized teleport** vector:

```
PR(u) = (1 - d) · p(u)
      + d · [  Σ_{v→u}  w(v,u) / W(v) · PR(v)        (link mass)
             + p(u) · Σ_{v ∈ dangling}  PR(v)  ]     (dangling mass)
```

- `d` = damping factor = **0.85**
- `p(u)` = teleport probability (default `1/N`; customizable for Personalized PageRank)
- `W(v)` = total out-weight of `v` (out-degree in the unweighted case)
- Convergence: `Σ_u |PR_new(u) − PR_old(u)| < ε` (default `0.001`)

This is exactly the formulation `networkx.pagerank` uses (with
`dangling = personalization`), which makes the two directly comparable.

---

## 3. Project structure

```
pagerank/
├── data/
│   ├── graph.txt              # Edge list (10 nodes, no dangling nodes)
│   ├── graph_dangling.txt     # Graph that contains dangling nodes
│   ├── graph_weighted.txt     # Weighted edge list (src, dst, weight)
│   └── graph_large.txt        # 1000-node scale-free graph (generated)
├── python/
│   ├── core/                  # Reference engine (dependency-free)
│   │   ├── pagerank_core.py   #   standard + weighted + personalized
│   │   └── cli.py             #   python -m core.cli ...
│   ├── mrjob/pagerank_mrjob.py
│   ├── pyspark/pagerank_spark.py
│   └── streaming/{init,mapper,reducer}.py
├── java/                      # Hadoop MapReduce (Maven)
├── pig/                       # Apache Pig Latin
├── scripts/                   # run_*.sh
├── tools/
│   ├── generate_graph.py      # scale-free graph generator
│   ├── benchmark.py           # benchmark + cross-check
│   └── visualize.py           # figure generation
├── tests/                     # pytest (core + cross-implementation)
├── docs/
│   ├── METHODOLOGY.md         # methodology & results
│   └── figures/               # generated figures
├── requirements.txt           # tooling dependencies
├── requirements-dev.txt       # test/framework dependencies
├── pyproject.toml             # packaging + pytest/ruff config
├── Dockerfile                 # reproducible container (Python 3.11)
├── .dockerignore
├── Makefile                   # make help
└── .github/workflows/ci.yml   # CI: ruff + pytest + docker build
```

---

## 4. Quick start

```bash
# 1. Install tooling dependencies
pip install -r requirements.txt          # or: make install

# 2. Run the reference engine
PYTHONPATH=python python -m core.cli data/graph.txt --iterations 20

# 3. Run the tests
pip install -r requirements-dev.txt
pytest                                   # or: make test
```

**Sample result on `data/graph.txt`** (10 nodes, converges in **13** iterations):

```
Node       PageRank
1          0.17582556
3          0.16908249
5          0.14970908
4          0.14811060
2          0.11174670
```

---

## 4.1 Docker (reproducible run)

A `Dockerfile` (Python 3.11, so every Python implementation including mrjob works)
is provided for a reproducible environment:

```bash
docker build -t pagerank .              # or: make docker-build
docker run --rm pagerank                # runs the test suite (skips the Spark test)
docker run --rm pagerank python -m core.cli data/graph.txt --iterations 20
```

PySpark is omitted from the image to keep it small (it normally runs on a cluster);
see the comment in the `Dockerfile` to enable it. The CI pipeline builds this image
and runs the tests inside it on every push.

---

## 5. Running each implementation

### 5.1 Reference engine (pure Python)

```bash
PYTHONPATH=python python -m core.cli data/graph.txt --iterations 20
PYTHONPATH=python python -m core.cli data/graph_large.txt --damping 0.9
# Personalized PageRank (all teleport mass on node 1):
PYTHONPATH=python python -m core.cli data/graph.txt --personalize 1=1.0
# Weighted PageRank (third column is the weight):
PYTHONPATH=python python -m core.cli data/graph_weighted.txt --weighted
```

### 5.2 Python — mrjob

```bash
# 'core' engine (default, exact):
python python/mrjob/pagerank_mrjob.py data/graph.txt --iterations 20

# 'mrjob' engine (genuine MapReduce decomposition, local via inline runner):
python python/mrjob/pagerank_mrjob.py data/graph.txt --iterations 20 --engine mrjob

# On a Hadoop cluster:
python python/mrjob/pagerank_mrjob.py hdfs:///graph.txt --engine mrjob --runner hadoop
```

### 5.3 Python — PySpark

```bash
./scripts/run_pyspark.sh both data/graph.txt 20      # rdd | df | both
# or:
spark-submit --master 'local[*]' python/pyspark/pagerank_spark.py \
    --input data/graph.txt --mode both --iterations 20
```

### 5.4 Python — Hadoop Streaming

```bash
# Local simulation (NO Hadoop needed):
./scripts/run_streaming.sh 20 --local data/graph.txt

# On a cluster (Hadoop running, graph on HDFS):
./scripts/run_streaming.sh 20
```

### 5.5 Java — Hadoop MapReduce

```bash
./scripts/run_java.sh 20          # requires Java 8+, Maven, Hadoop
```

### 5.6 Apache Pig

```bash
./scripts/run_pig.sh 20           # requires Pig + Hadoop
```

---

## 6. Tools

```bash
# Generate a scale-free graph (Barabási–Albert, m=3)
python tools/generate_graph.py --nodes 1000 --m 3 --output data/graph_1000.txt

# Comparative benchmark across all available frameworks, repeating each run 3x
# (mean ± std), writing output/benchmark_results.csv. Missing runtimes auto-skip.
python tools/benchmark.py --generate --sizes 1000 5000 10000 50000 \
    --frameworks core mrjob_core mrjob_mapreduce streaming --repeat 3 --iterations 30

# Regenerate the IEEE figures (vector PDF) into docs/figures/
python tools/visualize.py --csv output/benchmark_results.csv --input data/graph.txt
```

Figures (`docs/figures/`, vector PDF): `runtime_scaling.pdf`, `memory_scaling.pdf`,
`speedup.pdf`, `accuracy.pdf`, `convergence.pdf`, `top_ranks.pdf`. The scaling and
memory plots show standard-deviation error bars; the same files feed the paper
(`paper/figures/`).

---

## 7. Validation & accuracy

The reference engine matches `networkx.pagerank` in every mode; the distributed
implementations match the engine down to file-rounding precision (results are stored with
8 decimals). The table reports the **measured** maximum per-node absolute difference,
produced by `tests/` and `tools/benchmark.py`:

| Comparison | max \|Δ\| |
|---|---|
| `core` vs NetworkX — standard | 2.6 × 10⁻¹² |
| `core` vs NetworkX — personalized | 1.9 × 10⁻¹² |
| `core` vs NetworkX — weighted | 2.9 × 10⁻¹² |
| `core` vs NetworkX — 1000-node graph | 2.5 × 10⁻¹² |
| `core` vs NetworkX — dangling graph | 3.5 × 10⁻¹³ |
| mrjob (core / MapReduce) vs `core` | 5.0 × 10⁻⁹ |
| Hadoop Streaming (local) vs `core` | 7.9 × 10⁻⁹ |

```bash
make test          # pytest: core vs NetworkX + cross-implementation
pytest -m slow     # validation on a 400-node scale-free graph
```

---

## 8. Limitations

The **Java / Pig / Hadoop Streaming** family uses the standard single-pass formula and
**drops dangling-node mass** instead of redistributing it. They are therefore exact when
**every node has an out-link** (as in `data/graph.txt`). The **Python** family (`core`, the
mrjob `core` engine, PySpark) redistributes dangling mass correctly and is exact for any
graph. See `docs/METHODOLOGY.md` for details and remediation.

---

## 9. Paper

`paper/` contains an IEEE-format comparative study, *"A Reproducible Comparative Study
of PageRank Across the Hadoop Ecosystem,"* written from the **measured** results in this
repository. It establishes numerical equivalence between implementations first (the
validate-first discipline of Section 7), then compares wall-clock time, peak memory, and
convergence. The reference engine, the mrjob `core`/MapReduce engine, and Hadoop Streaming
are reported from real benchmark runs; PySpark, Java MapReduce, and Pig are analysed
architecturally and scoped explicitly as future work — no fabricated numbers.

| File | Role |
|---|---|
| `paper/main.tex` | Submission build (`\documentclass[conference]{IEEEtran}`) — needs `IEEEtran.cls` |
| `paper/preview.tex` | Proofreading build (`article` class + IEEEtran stubs) — compiles anywhere |
| `paper/content.tex` | Shared body: methodology, results, discussion, bibliography |
| `paper/abstract.tex` | Shared abstract (input by both builds) |
| `paper/figures/` | The five figures from `tools/visualize.py` (PDF) |
| `paper/preview.pdf` | Pre-compiled article-class preview |

```bash
make paper           # build paper/main.pdf (requires a TeX install with IEEEtran.cls)
make paper-preview   # build paper/preview.pdf (no IEEEtran.cls required)
```

Both targets use `latexmk` when available and fall back to two `pdflatex` passes (the
bibliography is embedded via `\thebibliography`, so no BibTeX step is needed).

---

## 10. References

- S. Brin and L. Page, "The anatomy of a large-scale hypertextual web search engine,"
  *Computer Networks and ISDN Systems*, vol. 30, no. 1–7, pp. 107–117, 1998.
- L. Page, S. Brin, R. Motwani, and T. Winograd, "The PageRank citation ranking: Bringing
  order to the web," Stanford InfoLab, Technical Report, 1999.
- A.-L. Barabási and R. Albert, "Emergence of scaling in random networks," *Science*,
  vol. 286, no. 5439, pp. 509–512, 1999.
- Apache Hadoop MapReduce Tutorial — https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
- PySpark RDD Programming Guide — https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Apache Pig Latin Reference — https://pig.apache.org/docs/latest/basic.html

---

## 11. License

- **Code** (the implementations, tools, and tests): MIT License — see [`LICENSE`](LICENSE).
- **Paper and figures** (`paper/`): Creative Commons Attribution 4.0 International
  (CC BY 4.0).

To cite this work, see [`CITATION.cff`](CITATION.cff); GitHub renders a
"Cite this repository" button from it.
