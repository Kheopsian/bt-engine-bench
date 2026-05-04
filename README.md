# bt-engine-bench

Cross-engine BitTorrent benchmark harness. Spawns each supported engine
headless via its native RPC, drives an identical workload, and exports
normalised metrics so you can plot apples-to-apples comparisons.

## Why it exists

There is no widely accepted BitTorrent engine benchmark. The "comparisons"
you can find online are forum posts that measure the wrapping client
(UI rendering, storage I/O, queue scheduler) rather than the engine,
or academic papers from 2005-2010 that simulated swarms in tools nobody
runs anymore. None of them touch the workloads that matter to
seedbox-class deployments — thousands of torrents seeded in parallel,
sparse peer activity, sustained throughput over hours.

This harness fills that gap.

## Engines covered

| Engine                            | Language | Concurrency model       | Driver mechanism            |
|-----------------------------------|----------|-------------------------|-----------------------------|
| [typhon](https://github.com/Kheopsian/Hydra) | Rust     | tokio async             | Unix socket JSON-RPC        |
| [rqbit](https://github.com/ikatson/rqbit)    | Rust     | state-machine           | HTTP API                    |
| [rain](https://github.com/cenkalti/rain)     | Go       | 1 goroutine per peer    | HTTP JSON-RPC 2.0           |
| [transmission](https://transmissionbt.com/)  | C        | daemon RPC              | HTTP w/ session-id refresh  |
| libtorrent (via [qBittorrent-nox](https://www.qbittorrent.org/)) | C++ | Boost.Asio | WebUI auth-bypass |
| [rtorrent](https://github.com/jesec/rtorrent) | C++     | xmlrpc-c                | SCGI XML-RPC                |

Every driver has been validated against the real engine — the wire format
documented in the source comments matches what the engine actually emits.

## Status

**Alpha**, publishable. The harness scaffolding is solid:

- All 6 drivers spawn, accept torrents, and report normalised stats
- 6-way idle smoke runs cleanly: each engine produces ~30 samples in a
  single run with consistent CSV schema
- Built-in BEP-3 HTTP tracker handles real announces from real engines
- Torrent generator builds valid `.torrent` files (round-tripped through rqbit)
- **All 6 engines transfer data** end-to-end in the same swarm.
  Reference 6-engine run (typhon seeds 50 MiB to five leechers, 180 s
  duration): every leecher reaches the full payload, and rqbit + typhon
  both re-upload to other peers in the swarm.

```
engine          UL total      DL total
typhon         157.3 MiB         0.0 B    ← seeder (served 3 leechers directly)
rqbit           50.0 MiB      50.0 MiB    ← re-upload to swarm
rain                0.0 B     50.0 MiB
libtorrent          0.0 B     50.0 MiB
rtorrent        50.1 MiB      50.2 MiB    ← also seeded back after completion
transmission        0.0 B     50.0 MiB
```

Caveat: rtorrent is the slowest engine to boot + announce, so short
scenarios (≤ 60 s) will frequently show it stuck at zero bytes simply
because typhon has already finished serving the faster handshakers
(rqbit, rain) before rtorrent enters the swarm. The bundled
`scenarios/swarm-seed.json` runs for 180 s precisely to give rtorrent
time to engage.

**Pending** (PRs welcome):

- `seed_mode` is wired through `TorrentSpec.Seed` and honoured by
  typhon. libtorrent (qbit-nox) has the same flag and just needs the
  driver to forward it.
- Multi-engine swarms could converge faster if the runner waited for
  every engine's BT listen socket to be live before kicking off Phase 2
  announces, instead of relying on each driver's own readiness
  heuristic.

## Quick start

```sh
# Build the harness binary.
go build -o bench ./cmd/bench

# Acquire engine binaries and images. typhon and rain need binary paths;
# the rest are pulled as Docker images on first run.
docker pull ikatson/rqbit:latest
docker pull linuxserver/transmission:latest
docker pull linuxserver/qbittorrent:latest
docker pull jesec/rtorrent:latest

# Run the 30-second idle smoke across all six engines.
./bench compare \
  --engines typhon,rqbit,transmission,libtorrent,rain,rtorrent \
  --scenario scenarios/smoke.json \
  --output run.csv \
  --typhon-bin /path/to/hydra-engine \
  --rain-bin /path/to/rain

# Plot the result.
pip install -r scripts/requirements.txt
python scripts/plot.py run.csv run.png
```

## Generating synthetic torrents

The harness ships a `gentorrent` helper for hand-built scenarios:

```sh
./bench gentorrent --random 10485760 --out 10mb.torrent --announce http://localhost:6969/announce
```

Inside scenarios (`scenarios/swarm-seed.json` is the reference example),
declare a synthetic swarm and let the runner build everything at run start:

```json
{
  "name": "10mb-typhon-seeds-everyone-leeches",
  "duration": "120s",
  "sample_interval": "1s",
  "tracker": "builtin",
  "swarm": [
    { "payload_size": 10485760, "seeders": ["typhon"] }
  ]
}
```

## Architecture

```
cmd/bench/main.go              CLI: compare, gentorrent
internal/
  engine/                      Driver interface + 6 implementations
  scenario/                    JSON scenario format
  runner/                      Orchestrates Start → AddTorrent → sample → Stop
  metrics/                     Flat-CSV writer with stable schema
  bencode/                     Minimal encoder (.torrent is bencoded)
  torrentgen/                  Builds .torrent files from a payload
  tracker/                     In-process BEP-3 HTTP tracker
scripts/
  plot.py                      pandas + matplotlib summariser
scenarios/                     Example scenario JSON files
```

Driver selection is closed over by `cmd/bench/main.go`. Adding a new engine
is a single file in `internal/engine/`, an entry in the `--engines` switch,
and a smoke test against the real engine.

## Design choices

**JSON, not YAML, for scenarios.** stdlib only, no parser dependency.

**One CSV per run.** Multiple engines share the file with an `engine` column.
Mixing scenarios in one CSV is supported via the `scenario` column. Plot
scripts split on those columns.

**Driver isolation.** Each engine gets its own data dir and listen port.
No two drivers share storage, ports, or peer pools — comparisons stay
unaffected by accidental cooperation.

**Engines own their config.** The harness exposes a small `StartConfig`
(data dir, listen port, on/off toggles for DHT/PEX/LSD, peer caps).
Anything beyond that is the engine's default. This avoids the trap where
the bench surreptitiously tunes one engine more aggressively than another.

## License

MIT. See `LICENSE`.
