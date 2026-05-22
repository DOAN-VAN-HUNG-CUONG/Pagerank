# Changelog

All notable changes are documented here. The format follows *Keep a Changelog*; the project
uses Semantic Versioning.

## [2.0.0] — Comprehensive upgrade

### Added
- **Reference engine** `python/core` (pure Python, no external dependencies): standard +
  **weighted** + **personalized** PageRank, correct dangling-node handling, returns the
  convergence history; with a CLI `python -m core.cli`.
- **pytest suite** (`tests/`): compares `core` against `networkx.pagerank` (standard,
  weighted, personalized, dangling), checks invariants (sum = 1, full node set, convergence),
  and **cross-validates** mrjob/Streaming/PySpark against `core`.
- **Tools** (`tools/`): `generate_graph.py` (Barabási–Albert scale-free graph),
  `benchmark.py` (timing + cross-check, JSON report), `visualize.py` (4 figures).
- **Datasets**: `data/graph_dangling.txt` (contains dangling nodes), `data/graph_weighted.txt`
  (weighted edge list), and `data/graph_large.txt` (1000 nodes, generated).
- **Packaging & CI**: `requirements.txt`, `requirements-dev.txt`, `pyproject.toml`
  (pytest/ruff config), `Makefile`, `.gitignore`, GitHub Actions (`ruff` + `pytest`).
- **Documentation**: rewritten `README.md`, and `docs/METHODOLOGY.md` (IEEE-style, with
  measured results).
- **Hadoop Streaming**: a local simulation mode `--local` (runs without Hadoop).
- **mrjob**: an `--engine {core,mrjob}` flag and a genuine, runnable single-pass MapReduce job.

### Fixed
- **PySpark RDD**: fixed dropped destination-only nodes, removed dead code (`if False`,
  `links.keys().collect()`), redistributes dangling mass correctly, uses `leftOuterJoin` to
  retain the full node set; fixed a **lazy-evaluation** risk where a closure captured the loop
  variable `base`.
- **PySpark DataFrame**: added dangling-mass redistribution (previously it leaked rank);
  replaced a fragile aliased self-join in the delta computation with explicit column renames.
- **PySpark I/O**: scheme-less paths now resolve to an absolute local `file://` URI, so
  `--input data/graph.txt` works even when the cluster's `fs.defaultFS` is HDFS (explicit
  `hdfs://`/`s3://` paths are respected).
- **PySpark DataFrame iteration**: each iteration now calls `localCheckpoint(eager=True)` to
  truncate the logical plan. Without it the chained joins build an ever-deeper plan that
  exhausts driver memory (`OutOfMemoryError` while rendering the plan tree / running AQE) and
  slows every iteration exponentially.
- **mrjob**: the local mode never used the MRJob classes; the `core` engine now reuses the
  verified core, and the MapReduce path was reimplemented so it genuinely runs.
- **Java** `PageRankIterJob`: activated the `OLDRANK:` branch to compute the **true
  convergence delta** `|new − old|` instead of the proxy `|new − 1/N|`; clarified the
  rank-initialization comment in `GraphInitJob` (self-normalization property).
- **Pig** `pagerank_iter.pig`: removed the dead-code block (`structure`,
  `ranks_with_structure`, `new_ranks_raw`) and the syntactically invalid
  `DEFINE teleport_term DIVIDE(...)` line.

### Removed
- Deleted the junk directory `{data,python/...}` created by a broken brace-expansion `mkdir`,
  and the `.DS_Store` files.

### Notes
- On `data/graph.txt` (no dangling nodes), all five implementations produce consistent
  results (`max|Δ| ~ 5 × 10⁻⁹`, limited by 8-decimal rounding). The Java/Pig/Streaming family
  drops dangling mass per the single-pass formula; see the Limitations section of
  `docs/METHODOLOGY.md`.
