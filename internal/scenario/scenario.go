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

	// Torrents is the workload: each entry will be added to every
	// engine the run covers.
	Torrents []TorrentEntry `json:"torrents"`

	// Constraints applied identically to every engine.
	Constraints Constraints `json:"constraints"`
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
