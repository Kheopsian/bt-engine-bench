// Package scenario defines the workload that drivers run identically.
//
// A scenario describes:
//   - how long the run lasts;
//   - how often the harness samples engine stats;
//   - which torrents each engine must register;
//   - any engine-agnostic constraints (DHT/PEX/LSD on or off, peer caps).
//
// The same Scenario value is fed to every driver. Engines that cannot honour
// a constraint exactly are expected to do their best and document the
// approximation in the run report.
package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Scenario is the wire format for a benchmark scenario file. Stored as
// JSON on disk; deliberately kept stdlib-only to avoid pulling YAML deps
// into the harness.
type Scenario struct {
	// Name is a short human-readable identifier. Used as a column in
	// the output CSV so a single CSV can mix runs.
	Name string `json:"name"`

	// Duration is the total wall-clock time the run lasts.
	Duration Duration `json:"duration"`

	// SampleInterval is how often the harness reads Stats() from each
	// engine.
	SampleInterval Duration `json:"sample_interval"`

	// Torrents is the legacy workload: each entry refers to a .torrent
	// file that already exists on disk. Engines load the torrent but
	// the harness does NOT pre-populate any payload — useful for "idle
	// engine" measurement, useless for throughput scenarios.
	Torrents []TorrentEntry `json:"torrents"`

	// Tracker controls the bench's in-process HTTP tracker. Values:
	//   ""        — disabled
	//   "builtin" — runner starts a tracker on TrackerListen and
	//               embeds its URL into every Swarm-generated .torrent
	Tracker string `json:"tracker"`

	// TrackerListen is the host:port the builtin tracker binds to.
	// Defaults to "0.0.0.0:6969". The runner advertises the same port
	// to engines via the announce URL.
	TrackerListen string `json:"tracker_listen"`

	// Swarm describes torrents the harness should generate at runtime.
	// Each entry produces one synthetic .torrent shared across every
	// engine in the run. Seeders are pre-populated with the full
	// payload before AddTorrent — they start at 100% and only upload.
	// Other engines start at 0% and leech.
	Swarm []SwarmEntry `json:"swarm"`

	// Constraints applied identically to every engine.
	Constraints Constraints `json:"constraints"`
}

// SwarmEntry describes one synthetic torrent the runner generates for the
// scenario. Either PayloadSize (random bytes generated at run start) or
// PayloadPath (existing file) is required.
type SwarmEntry struct {
	// PayloadSize, in bytes. The runner writes a random payload of
	// this size to the run's working directory. Mutually exclusive
	// with PayloadPath.
	PayloadSize int64 `json:"payload_size"`

	// PayloadPath is an existing file on disk to wrap. Path must be
	// readable from the harness host.
	PayloadPath string `json:"payload_path"`

	// PieceLength forwarded to torrentgen. Zero means default
	// (256 KiB). Tune for scenarios that care about piece-pipeline
	// behaviour.
	PieceLength int64 `json:"piece_length"`

	// Seeders is the list of engine names that start the run already
	// holding a complete copy of the payload. Engines NOT in this
	// list start empty and have to download. Empty seeders means
	// nobody seeds — the swarm will sit idle.
	Seeders []string `json:"seeders"`
}

// TorrentEntry describes one torrent the harness should hand to each engine.
type TorrentEntry struct {
	// File is the path to the .torrent file on the harness host.
	File string `json:"file"`

	// SavePath is the directory the engine should use as the payload
	// root. Path is interpreted relative to each engine's working
	// directory unless absolute.
	SavePath string `json:"save_path"`
}

// Constraints captures the knobs we want every engine to respect.
type Constraints struct {
	MaxPeersPerTorrent  int  `json:"max_peers_per_torrent"`
	MaxTotalConnections int  `json:"max_total_connections"`
	DisableDHT          bool `json:"disable_dht"`
	DisablePEX          bool `json:"disable_pex"`
	DisableLSD          bool `json:"disable_lsd"`
}

// Duration wraps time.Duration with JSON support so scenarios can write
// "60s" instead of nanosecond integers.
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("scenario: invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Load reads a scenario file from disk and applies sensible defaults.
func Load(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("scenario: read %s: %w", path, err)
	}
	var sc Scenario
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, fmt.Errorf("scenario: parse %s: %w", path, err)
	}
	if sc.SampleInterval == 0 {
		sc.SampleInterval = Duration(1 * time.Second)
	}
	if sc.Duration == 0 {
		return nil, fmt.Errorf("scenario: duration is required")
	}
	return &sc, nil
}
