# Faster routing and output for SUMO — CCH + parallel FCD

**Repo:** https://github.com/stprnvsh/sumo-fastrouting
**Branch (has both features):** [`feature/parallel-fcd`](https://github.com/stprnvsh/sumo-fastrouting/tree/feature/parallel-fcd)
**Install / build / test instructions:** [`README_CCH.md`](https://github.com/stprnvsh/sumo-fastrouting/blob/feature/parallel-fcd/README_CCH.md)

This is a fork of [Eclipse SUMO](https://github.com/eclipse-sumo/sumo) with two
independent, opt-in changes that make large-scale meso simulation with live
travel-time output substantially faster: a **Customizable Contraction
Hierarchies (CCH) routing backend**, and **parallelized FCD (Floating Car
Data) output**. Both are additive — a build without them behaves exactly like
upstream SUMO.

### Branch history

The work landed as three branches, each a strict continuation of the last —
`feature/parallel-fcd` contains everything below it, so it's the one to build
from unless you specifically want a smaller diff:

1. **[`feature/cch-routing-backend`](https://github.com/stprnvsh/sumo-fastrouting/tree/feature/cch-routing-backend)**
   — the CCH routing backend on its own: the RoutingKit-based `--routing-algorithm
   CCH` router, `weights.priority-factor` support, and the build-guard fix for
   Parquet/Arrow header conflicts. This is where all of the routing work
   described in §1.1 and the 25×/87%-exactness/closure numbers in §2 come
   from. Use this branch if you want CCH **without** touching FCD output at
   all — it's the minimal diff against upstream for the routing change alone.
2. **[`feature/parquet-async-writer`](https://github.com/stprnvsh/sumo-fastrouting/tree/feature/parquet-async-writer)**
   — built on top of (1). Adds typed Parquet column appends and moves Parquet
   writing onto a background thread, the groundwork the full row-staging
   mechanism in §1.2 builds on.
3. **[`feature/parallel-fcd`](https://github.com/stprnvsh/sumo-fastrouting/tree/feature/parallel-fcd)**
   — built on top of (2). Adds parallel per-vehicle FCD value computation
   (`--fcd-output.threads`) and the row-stager worker-thread mechanism. This
   is the branch referenced throughout the rest of this document and the one
   the install instructions below use.

---

## 1. What we changed

### 1.1 CCH routing backend (`--routing-algorithm CCH`)

SUMO's built-in routers answer "what's the fastest path right now" one of two
ways: **A\*** re-searches the graph from scratch on every query, or the
built-in **CH** pre-computes shortcuts once and answers in microseconds — but
those shortcuts are baked to a *fixed* set of edge weights, so the moment
travel times change (which happens continuously in meso, from live
congestion), CH has to fully re-build itself. On a live city network with
weights updating every few minutes, that pushes CH back toward "too slow to
use."

**What we implemented:** a routing backend built on
[RoutingKit](https://github.com/RoutingKit/RoutingKit)'s Customizable
Contraction Hierarchies. CCH splits routing into two phases instead of one:

- **Topology** (which shortcuts exist) — depends only on the road network's
  shape, built **once**, at startup.
- **Customization** (how long each shortcut takes) — a cheap, few-millisecond
  re-computation from the *current* live edge speeds, run every time weights
  update.

Because the expensive part (deciding the shortcut structure) never has to be
redone, queries stay in the microsecond range **even while the network's
congestion state keeps changing** — the exact situation SUMO's own weight
updates create.

Three implementation details worth calling out, because they're the reason
this works correctly on a real city network rather than just a toy graph:

- **Zone connectors (TAZ) are handled as query-time endpoint sets, not graph
  nodes.** A zone connector touches every edge in its zone, and folding it
  directly into the hierarchy poisons the whole structure (provably — it's a
  treewidth blow-up, not a tuning issue). Instead, each zone is resolved at
  query time as a set of real edges (the same trick OSRM calls "phantom
  nodes"), using RoutingKit's native multi-source/multi-target query.
- **One shared hierarchy serves every vehicle class.** Cars, trucks, buses,
  and trams differ only in which road connections they're allowed to use —
  not in travel-time formula — so a single topology serves all of them; a
  per-class mask marks forbidden connections as impassable in each class's
  own customization.
- **Road closures need no special handling.** A closure is just a permission
  change on a connection. Since permissions are already folded into the
  per-class customization step, a closed road is automatically "impassable"
  the next time weights refresh (about once a second in a typical config) —
  no separate closure-handling code path exists or is needed.

### 1.2 Parallel FCD output (`--fcd-output.threads N`)

FCD output (per-vehicle position/speed/state, written every step) is one of
SUMO's most expensive outputs at scale — every vehicle, every step, needs its
field values computed and serialized. Historically this was single-threaded
and fully sequential with the simulation step.

**What we implemented, in two parts:**

- **Parallel value computation** (`--fcd-output.threads N`): the per-vehicle
  field values (position, speed, edge, queue, etc.) for a simulation step are
  computed across `N` worker threads instead of one.
- **A staged writer thread** (the row-stager): rather than blocking the main
  simulation loop while rows are serialized to disk, each row is staged by a
  worker into a lightweight formatter-specific buffer, then appended back in
  original order on the owning thread. This decouples "compute the row" from
  "write the row" without changing output order or format.

Both are formatter-agnostic (implemented for Parquet and plain XML) and
produce **byte-identical output** to the original single-threaded path —
this was verified directly: SHA-256 hashes of the async-reference output and
the parallel output (6 and 8 threads) are identical.

---

## 2. Why this helps — measured, not estimated

Every number below was measured in this session, not projected. Scenario:
Geneva, a real 24-hour meso network (~554,000 trips/day, ~13,700 routable
edges).

### Routing: CCH vs. A\*

| | A\* (baseline) | CCH |
|---|---|---|
| routing CPU, 1-hour window | 62 s | **2.5 s (25× faster)** |
| per-query cost | ~0.6 ms, ~2,400 edges explored | ~0.01 ms, ~40 edges explored |
| fallback to A\* (queries CCH can't express) | — | 0.29% |
| route match vs. exact A\* | — | 87.0% exact; mean cost difference 0.65% |
| closure correctness | — | 0 vehicles cross a closed road, in both a same-step and a mid-run closure test |

### End-to-end: CCH + parallel FCD vs. stock SUMO, full day

We built a genuine vanilla SUMO baseline from the exact commit this fork
diverged from (no CCH, no parallel FCD) and ran the same 24-hour Geneva
scenario, same seed, same vehicle demand, on both:

| | vanilla SUMO (A\*, single-threaded FCD) | this fork (CCH + parallel FCD) |
|---|---|---|
| wall clock, full day (~547,000 vehicles) | 2406 s (40 min) | **427 s (7 min) — 5.6× faster** |
| routing queries | 2.39M, avg 2,031 edges explored | 2.39M, avg **41 edges explored** |
| time spent routing | 1150 s (**48% of total runtime**) | ~30 s |
| FCD rows written | — | 452,099,717 (validated, all row groups readable) |

One additional finding from this same test: stock SUMO's multi-threaded A\*
router segfaulted partway through the full-day run (a race in its
pending-vehicle-removal code, triggered by a high volume of route-removal
events over a long run) — single-threaded A\* and CCH at any thread count
both ran the full day cleanly. So beyond being faster, CCH sidesteps a
stability issue that only shows up under sustained multi-threaded A\* load.

### Where the speedup actually comes from

Threading alone (more `device.rerouting.threads`) buys real but limited
gains — about 1.6× in our tests — because it only parallelizes *how many*
expensive per-query searches run at once; each query still explores
thousands of edges. **CCH's win is algorithmic, not parallelism**: it makes
each query itself cheap (~40 edges instead of ~2,000+), which is why adding
more routing threads to the CCH path barely moves the needle — there's very
little per-query cost left to parallelize away. The two techniques compose
(you can use both), but the algorithm change is where the actual 25×/5.6×
comes from.

---

## 3. How to get it and try it

```bash
# 1. Clone this fork and check out the branch with both features
git clone https://github.com/stprnvsh/sumo-fastrouting.git
cd sumo-fastrouting
git checkout feature/parallel-fcd

# 2. Follow README_CCH.md for the full build (RoutingKit dependency,
#    CMake flags, platform prerequisites):
#    https://github.com/stprnvsh/sumo-fastrouting/blob/feature/parallel-fcd/README_CCH.md

# 3. Quick build summary (macOS/Homebrew shown; see the README for Linux/Windows)
brew install cmake ninja xerces-c fox proj gdal gl2ps libomp
git clone https://github.com/RoutingKit/RoutingKit.git
cd RoutingKit && ./generate_make_file && make && cd ..

cmake -B build -G Ninja \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DSUMO_WITH_CCH=ON \
      -DROUTINGKIT_ROOT=/absolute/path/to/RoutingKit
cmake --build build --target sumo -j

# 4. Verify both features are compiled in
build/bin/sumo --version   # "Build features" line should include CCH
build/bin/sumo --help | grep -E "routing-algorithm|fcd-output.threads"

# 5. Run a scenario with both
build/bin/sumo -c yourscenario.sumocfg \
    --routing-algorithm CCH \
    --fcd-output out.parquet --fcd-output.threads 6
```

Both features are fully opt-in: omit `-DSUMO_WITH_CCH=ON` and the build needs
no RoutingKit dependency at all; omit `--fcd-output.threads` and FCD output
runs exactly as it does upstream.

## 4. Current limitations (honest, from the README)

- CCH's per-query "extras" that need a *different* metric per vehicle
  (`weights.random-factor != 1`, bike speeds, per-vehicle routing
  preferences) aren't representable in a shared hierarchy and are rejected
  up front — disable them to use `--routing-algorithm CCH`.
- OpenMP (`libomp`) is optional but recommended: without it, per-interval
  customization runs serially (~2.7 ms/class on Geneva) instead of in
  parallel — correct either way, just slower to re-customize.
- This is a feature branch, not yet an upstream Eclipse SUMO pull request —
  it doesn't vendor the RoutingKit dependency in-tree or ship
  upstream-style regression tests/ChangeLog entries.
