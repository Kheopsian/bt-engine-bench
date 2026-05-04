# bt-engine-bench

Cross-engine BitTorrent benchmark harness. Spawns each engine headless via
its native RPC, drives an identical workload, and exports normalised metrics
for direct comparison.

## Why

There is no widely accepted BitTorrent engine benchmark. Existing comparisons
are anecdotal forum posts measuring the wrapping client (UI, storage I/O,
queue scheduler) rather than the engine itself. Academic interest peaked
around 2005-2010 with simulation-based work that never reflected real
implementations.

Worse: published numbers focus on the easy case — single torrent, max
throughput, foreground download. The interesting workloads (5k-15k torrents
seeding simultaneously with sparse peer activity) are never measured.

This harness fills that gap.

## Design

- Each engine is treated as a black box driven through its native API
  (libtorrent C++ via qbittorrent-nox Web API, rqbit HTTP, transmission RPC,
  typhon JSON-RPC over Unix socket).
- Engines run in Docker containers for reproducibility.
- A shared scenario specification injects identical torrent sets, peer pools,
  and timing into every engine.
- Metrics are collected at fixed intervals from the engine's own stats API
  and normalised into a flat CSV. No engine-specific knobs leak into the
  output schema.
- Plot scripts (Python) render comparative charts from the CSV.

## Status

WIP. v1 target: typhon + rqbit + transmission + libtorrent (via qbit-nox).

## Engines covered

| Engine        | Driver mechanism            | Status |
|---------------|-----------------------------|--------|
| typhon        | JSON-RPC over Unix socket   | TODO   |
| rqbit         | HTTP API                    | TODO   |
| transmission  | RPC daemon                  | TODO   |
| libtorrent    | qbittorrent-nox Web API     | TODO   |

## Usage

```
bench compare \
  --engines typhon,rqbit \
  --scenario scenarios/hoard_seed_1k.yaml \
  --duration 300s \
  --output run.csv
```

## License

MIT. See `LICENSE`.
