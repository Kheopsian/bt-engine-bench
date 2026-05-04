// Package runner orchestrates a single scenario across one or more engine
// drivers. Each engine runs in isolation (its own data dir, its own
// containerised process) but all of them are fed the same torrent set on
// the same timeline.
package runner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Kheopsian/bt-engine-bench/internal/engine"
	"github.com/Kheopsian/bt-engine-bench/internal/metrics"
	"github.com/Kheopsian/bt-engine-bench/internal/scenario"
	"github.com/Kheopsian/bt-engine-bench/internal/torrentgen"
	"github.com/Kheopsian/bt-engine-bench/internal/tracker"
)

// Run executes the scenario across every supplied driver and writes samples
// to out. Each driver is brought up sequentially so any engine-specific
// startup log lands in the harness output without interleaving.
//
// Returns the first non-cancellation error encountered. Engines are always
// torn down on the way out, even if the scenario errored mid-run.
func Run(ctx context.Context, sc *scenario.Scenario, drivers []engine.Driver, workDir string, out *metrics.Writer) error {
	if len(drivers) == 0 {
		return fmt.Errorf("runner: at least one driver required")
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return fmt.Errorf("runner: mkdir workDir: %w", err)
	}

	// Phase 0 — bring up the in-process tracker if requested. Doing this
	// before the engines start ensures the announce URL is reachable by
	// the time AddTorrent fires the first announce.
	var trackerURL string
	if sc.Tracker == "builtin" {
		listen := sc.TrackerListen
		if listen == "" {
			listen = "0.0.0.0:6969"
		}
		t, err := tracker.Start(listen)
		if err != nil {
			return fmt.Errorf("runner: start tracker: %w", err)
		}
		defer t.Stop()
		// Engines need a URL they can actually reach. Even if the
		// tracker bound to 0.0.0.0:port, advertise via 127.0.0.1 so
		// engines on host loopback connect cleanly. Container engines
		// will need host networking to share that loopback view.
		_, port, _ := splitHostPort(listen)
		trackerURL = fmt.Sprintf("http://127.0.0.1:%s/announce", port)
		log.Printf("runner: tracker started, announce URL = %s", trackerURL)
	}

	// Phase 0.5 — materialise synthetic torrents from sc.Swarm. Each
	// SwarmEntry produces one shared .torrent that every engine in the
	// run will get fed. The payload either pre-exists (PayloadPath) or
	// gets a freshly randomised buffer (PayloadSize).
	var swarmTorrents []scenario.TorrentEntry
	for i, sw := range sc.Swarm {
		torrentPath, err := materialiseSwarm(workDir, i, sw, trackerURL)
		if err != nil {
			return fmt.Errorf("runner: swarm[%d]: %w", i, err)
		}
		swarmTorrents = append(swarmTorrents, scenario.TorrentEntry{
			File:     torrentPath,
			SavePath: "swarm",
		})
	}

	// Phase 1 — bring up each engine. We assign each one a unique data
	// dir and a non-overlapping listen port. Listen ports are taken
	// from a base + offset; the harness assumes nothing else on the
	// host is using 17000-17099.
	type running struct {
		driver  engine.Driver
		dataDir string
	}
	var live []running
	defer func() {
		// Always stop in reverse order. We use a fresh context so a
		// cancelled run can still tear down cleanly.
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for i := len(live) - 1; i >= 0; i-- {
			if err := live[i].driver.Stop(stopCtx); err != nil {
				log.Printf("runner: %s stop: %v", live[i].driver.Name(), err)
			}
		}
	}()

	for i, d := range drivers {
		dataDir := filepath.Join(workDir, d.Name())
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return fmt.Errorf("runner: mkdir %s: %w", dataDir, err)
		}
		startCfg := engine.StartConfig{
			DataDir:             dataDir,
			ListenPort:          17000 + i,
			MaxPeersPerTorrent:  sc.Constraints.MaxPeersPerTorrent,
			MaxTotalConnections: sc.Constraints.MaxTotalConnections,
			DisableDHT:          sc.Constraints.DisableDHT,
			DisablePEX:          sc.Constraints.DisablePEX,
			DisableLSD:          sc.Constraints.DisableLSD,
		}
		log.Printf("runner: starting %s (data=%s port=%d)", d.Name(), dataDir, startCfg.ListenPort)
		startCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := d.Start(startCtx, startCfg)
		cancel()
		if err != nil {
			return fmt.Errorf("runner: start %s: %w", d.Name(), err)
		}
		live = append(live, running{driver: d, dataDir: dataDir})
	}

	// Phase 2 — register torrents on every engine. Each torrent's save
	// path lives under the per-engine dataDir so that engines do not
	// share storage and cannot accidentally cooperate via shared files.
	allTorrents := append([]scenario.TorrentEntry{}, sc.Torrents...)
	allTorrents = append(allTorrents, swarmTorrents...)
	for _, r := range live {
		for _, t := range allTorrents {
			meta, err := os.ReadFile(t.File)
			if err != nil {
				return fmt.Errorf("runner: read torrent %s: %w", t.File, err)
			}
			savePath := t.SavePath
			if !filepath.IsAbs(savePath) {
				savePath = filepath.Join(r.dataDir, savePath)
			}
			spec := engine.TorrentSpec{
				// InfoHash is left empty: drivers that need it
				// derive it from the torrent metadata themselves
				// (typhon writes the .torrent to disk with a
				// time-suffixed name when InfoHash is empty).
				MetaBytes: meta,
				SavePath:  savePath,
			}
			addCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err = r.driver.AddTorrent(addCtx, spec)
			cancel()
			if err != nil {
				return fmt.Errorf("runner: %s add_torrent %s: %w", r.driver.Name(), t.File, err)
			}
		}
	}

	// Phase 3 — sampling loop. Each driver gets its own goroutine so
	// a slow Stats() call on one engine cannot stall the others.
	runCtx, cancel := context.WithTimeout(ctx, sc.Duration.Std())
	defer cancel()

	var wg sync.WaitGroup
	for _, r := range live {
		wg.Add(1)
		go func(r running) {
			defer wg.Done()
			t := time.NewTicker(sc.SampleInterval.Std())
			defer t.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case ts := <-t.C:
					sCtx, cancel := context.WithTimeout(runCtx, sc.SampleInterval.Std())
					stats, err := r.driver.Stats(sCtx)
					cancel()
					if err != nil {
						log.Printf("runner: %s stats: %v", r.driver.Name(), err)
						continue
					}
					if err := out.WriteSample(ts, r.driver.Name(), stats); err != nil {
						log.Printf("runner: csv write: %v", err)
					}
				}
			}
		}(r)
	}
	wg.Wait()
	return nil
}

// materialiseSwarm generates one synthetic .torrent for a SwarmEntry and
// writes it under workDir/swarm/<idx>.torrent. The payload either pre-exists
// (caller-supplied path) or is freshly randomised in workDir/swarm/<idx>.bin.
// Returns the path to the .torrent (which the runner then loads into every
// engine just like a manually-supplied torrent).
func materialiseSwarm(workDir string, idx int, sw scenario.SwarmEntry, trackerURL string) (string, error) {
	swarmDir := filepath.Join(workDir, "swarm")
	if err := os.MkdirAll(swarmDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir swarm dir: %w", err)
	}

	payloadPath := sw.PayloadPath
	if payloadPath == "" {
		if sw.PayloadSize <= 0 {
			return "", fmt.Errorf("either payload_path or payload_size must be set")
		}
		// Hex suffix keeps multi-swarm scenarios distinguishable on
		// disk; the bytes inside are still freshly random per run so
		// the info_hash differs every invocation.
		suffix := make([]byte, 4)
		_, _ = rand.Read(suffix)
		payloadPath = filepath.Join(swarmDir, fmt.Sprintf("payload-%d-%s.bin", idx, hex.EncodeToString(suffix)))
		f, err := os.Create(payloadPath)
		if err != nil {
			return "", fmt.Errorf("create payload: %w", err)
		}
		if _, err := io.CopyN(f, rand.Reader, sw.PayloadSize); err != nil {
			f.Close()
			return "", fmt.Errorf("write random payload: %w", err)
		}
		f.Close()
	}

	res, err := torrentgen.GenerateFile(torrentgen.Spec{
		PayloadPath: payloadPath,
		PieceLength: sw.PieceLength,
		AnnounceURL: trackerURL,
	})
	if err != nil {
		return "", err
	}
	torrentPath := filepath.Join(swarmDir, fmt.Sprintf("%d.torrent", idx))
	if err := os.WriteFile(torrentPath, res.Torrent, 0o644); err != nil {
		return "", fmt.Errorf("write torrent: %w", err)
	}
	log.Printf("runner: swarm[%d] generated: payload=%s torrent=%s info_hash=%x size=%d",
		idx, payloadPath, torrentPath, res.InfoHash, res.Size)
	return torrentPath, nil
}

// splitHostPort accepts both "host:port" and ":port" and returns the two
// components. We do not use net.SplitHostPort here because the empty-host
// case we care about ("0.0.0.0:6969" giving "6969") works fine with raw
// string slicing and avoids a dependency on the package for one call.
func splitHostPort(s string) (host, port string, ok bool) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return "", s, false
}
