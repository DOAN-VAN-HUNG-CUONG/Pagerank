# Methodology and Evaluation — PageRank on the Hadoop Ecosystem

> IEEE-style academic writing. Every quantitative figure is measured directly by `tests/`
> and `tools/benchmark.py`; no results are fabricated.

---

## I. Introduction

PageRank assigns each node of a directed graph a score proportional to the stationary
probability of a damped random walk. This report presents a pure-Python reference engine
alongside five Hadoop-ecosystem implementations, and a validation procedure that guarantees
numerical consistency between them.

---

## II. Algorithm formulation

Let `N` be the number of nodes, `d` the damping factor, `p(u)` the teleport distribution
(uniform `1/N` by default), `W(v)` the total out-weight of `v`, and `D` the set of dangling
nodes (no out-links). The general one-iteration update is:

```
PR_{k+1}(u) = (1 - d)·p(u) + d·[ Σ_{v→u} (w(v,u)/W(v))·PR_k(v) + p(u)·Σ_{v∈D} PR_k(v) ]
```

Three special cases follow: (i) *standard* — unweighted, uniform `p`; (ii) *weighted* —
`w(v,u)` is the edge weight; (iii) *personalized* — `p` is an arbitrary probability vector.
The last term redistributes dangling mass according to `p`, keeping the total score equal to
1. This is exactly the form `networkx.pagerank` uses (with `dangling = personalization`).

*Self-normalization.* The total score `S_k = Σ_u PR_k(u)` satisfies
`S_{k+1} = (1-d) + d·S_k`, whose unique fixed point is `S = 1`. The total therefore converges
to 1 regardless of the initial value — this property explains why the Java implementation
(which initializes rank = 1.0) still converges correctly.

---

## III. Implementations

The reference engine (`python/core`) is pure Python with no third-party dependency,
implements all three variants, and returns the convergence history. The five distributed
implementations:

1. **mrjob** — two paths: the exact `core` engine, and a genuine single-pass MapReduce job
   (`PageRankIteration`) runnable via the inline/local/Hadoop runners.
2. **PySpark** — two styles: RDD and DataFrame; both redistribute dangling mass.
3. **Hadoop Streaming** — `init | mapper | sort | reducer`, with a local simulation mode.
4. **Java MapReduce** — a three-stage driver (init → iterate → sort), convergence via a
   counter.
5. **Apache Pig** — Pig Latin: init, iterate, sort.

*Dangling design.* The Python family (core, mrjob `core`, PySpark) redistributes dangling
mass and is exact for any graph. The single-pass JVM/Streaming family (Java, Pig, Streaming)
drops dangling mass — exact when every node has an out-link. Global redistribution within a
single MapReduce pass requires a separate aggregation/broadcast step (see Section VII).

---

## IV. Experimental methodology

*Datasets.* (a) `graph.txt` — 10 nodes, 22 edges, no dangling; (b) `graph_dangling.txt` — 6
nodes with dangling nodes; (c) `graph_large.txt` — a 1000-node scale-free graph from the
Barabási–Albert model (`tools/generate_graph.py`, fixed seed for reproducibility). *Ground
truth*: `networkx.pagerank` with `tol = 1e-12`.

*Procedure.* Each implementation runs with `d = 0.85`. For comparison, early-stopping
implementations are compared against `core` at the same `ε`; fixed-iteration implementations
(Streaming) are compared against `core` run for that exact iteration count. This removes
differences caused by distinct stopping points and measures only genuine algorithmic
divergence.

---

## V. Evaluation metrics

(1) **Maximum per-node absolute error** vs ground truth: `max_u |PR(u) − PR_ref(u)|`.
(2) **L1 convergence error**: `Σ_u |PR_{k+1}(u) − PR_k(u)|` per iteration.
(3) **Iterations to convergence** at `ε`.
(4) **Wall-clock time** — hardware-dependent, used for relative comparison, not as an
accuracy metric.

---

## VI. Results

The reference engine matches `networkx.pagerank` to order `10⁻¹²` across all three variants
and on the 1000-node graph; on the dangling graph, the total is `1.00000000` and the error
vs NetworkX is `3.5 × 10⁻¹³`. The distributed implementations runnable in the test
environment (both mrjob paths, local Streaming) match `core` at `~10⁻⁹`, exactly the
8-decimal file-rounding limit. On `graph.txt`, the algorithm converges after **13**
iterations at `ε = 0.001`.

**Table I — Maximum per-node absolute error**

| Comparison | max \|Δ\| |
|---|---|
| `core` vs NetworkX — standard (`graph.txt`) | 2.62 × 10⁻¹² |
| `core` vs NetworkX — personalized | 1.88 × 10⁻¹² |
| `core` vs NetworkX — weighted | 2.87 × 10⁻¹² |
| `core` vs NetworkX — 1000-node scale-free | 2.52 × 10⁻¹² |
| `core` vs NetworkX — dangling graph | 3.53 × 10⁻¹³ |
| mrjob (`core` engine) vs `core` | 4.98 × 10⁻⁹ |
| mrjob (MapReduce engine) vs `core` | 4.98 × 10⁻⁹ |
| Hadoop Streaming (local) vs `core` | 7.85 × 10⁻⁹ |

Figure `docs/figures/convergence.png` shows the L1 delta decreasing monotonically per
iteration; `docs/figures/graph.png` shows nodes 1, 3, 5 with the highest PageRank,
consistent with Table I.

---

## VII. Limitations

(1) *Dangling in the single-pass family.* Java, Pig and Hadoop Streaming drop dangling mass;
they are exact only when every node has an out-link. The standard remedy is to add a global
counter/aggregation that sums dangling mass each iteration and broadcasts it back — this is
future work, deliberately omitted to keep the single-pass architecture simple.

(2) *Validation scope.* In the sandbox, the PySpark, Java (Maven/Hadoop) and Pig parts were
**not executed** (PySpark needs a ~455 MB download; Java/Pig need a Hadoop cluster). They
were code-reviewed and syntax-checked; the corresponding tests skip automatically when the
runtime is absent and will run on the user's machine.

(3) *Storage precision.* Result files store 8 decimals, capping cross-implementation
agreement at `~5 × 10⁻⁹`.

---

## VIII. Future work

(1) Redistribute dangling mass in Java/Pig/Streaming via a global aggregation step.
(2) Evaluate scalability on larger graphs (10⁶–10⁷ edges) and measure Spark strong/weak
scaling.
(3) Add Topic-Sensitive PageRank and compare against GraphFrames.
(4) Expose `epsilon`/`damping` on the command line for every implementation.

---

## IX. References

[1] S. Brin and L. Page, "The anatomy of a large-scale hypertextual web search engine,"
*Computer Networks and ISDN Systems*, vol. 30, no. 1–7, pp. 107–117, 1998.

[2] L. Page, S. Brin, R. Motwani, and T. Winograd, "The PageRank citation ranking: Bringing
order to the web," Stanford InfoLab, Technical Report 1999-66, 1999.

[3] A.-L. Barabási and R. Albert, "Emergence of scaling in random networks," *Science*,
vol. 286, no. 5439, pp. 509–512, 1999.

[4] J. Dean and S. Ghemawat, "MapReduce: Simplified data processing on large clusters,"
*Communications of the ACM*, vol. 51, no. 1, pp. 107–113, 2008.

[5] A. Hagberg, D. Schult, and P. Swart, "Exploring network structure, dynamics, and
function using NetworkX," in *Proc. 7th Python in Science Conf. (SciPy)*, 2008, pp. 11–15.
