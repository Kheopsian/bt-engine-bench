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
	// gets a freshly randomised buffer (PayloadSize). Seeders are
	// remembered so that Phase 2 can stage the payload on those engines
	// before issuing AddTorrent.
	type swarmArtefact struct {
		entry       scenario.TorrentEntry
		payloadPath string
		torrentName string
		seeders     map[string]bool
	}
	var artefacts []swarmArtefact
	for i, sw := range sc.Swarm {
		torrentPath, payloadPath, torrentName, err := materialiseSwarm(workDir, i, sw, trackerURL)
		if err != nil {
			return fmt.Errorf("runner: swarm[%d]: %w", i, err)
		}
		seeders := map[string]bool{}
		for _, name := range sw.Seeders {
			seeders[name] = true
		}
		artefacts = append(artefacts, swarmArtefact{
			entry: scenario.TorrentEntry{
				File:     torrentPath,
				SavePath: "swarm",
			},
			payloadPath: payloadPath,
			torrentName: torrentName,
			seeders:     seeders,
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
	// For swarm torrents whose scenario lists this engine as a seeder,
	// the payload is staged at the driver-specific SeedPath BEFORE
	// AddTorrent — verify will then mark the torrent complete and the
	// engine starts seeding instead of leeching.
	for _, r := range live {
		// Legacy torrents (file-on-disk, harness does no payload
		// staging) — added as-is.
		for _, t := range sc.Torrents {
			if err := addOne(ctx, r.driver, r.dataDir, t); err != nil {
				return err
			}
		}
		// Swarm torrents — opportunistically seed.
		for _, art := range artefacts {
			savePath := filepath.Join(r.dataDir, art.entry.SavePath)
			if err := os.MkdirAll(savePath, 0o755); err != nil {
				return fmt.Errorf("runner: mkdir save_path: %w", err)
			}
			if art.seeders[r.driver.Name()] {
				seeder, ok := r.driver.(engine.Seeder)
				if !ok {
					log.Printf("runner: %s does not implement Seeder, skipping pre-populate", r.driver.Name())
				} else {
					dst := seeder.SeedPath(savePath, art.torrentName)
					if err := copyFile(art.payloadPath, dst); err != nil {
						return fmt.Errorf("runner: stage seed for %s: %w", r.driver.Name(), err)
					}
					log.Printf("runner: staged seed for %s at %s", r.driver.Name(), dst)
				}
			}
			if err := addOne(ctx, r.driver, r.dataDir, art.entry); err != nil {
				return err
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
// Returns the .torrent path, the payload path on disk, and the torrent's
// info.name (= payload basename) — all three are needed by the runner to
// stage seeders.
func materialiseSwarm(workDir string, idx int, sw scenario.SwarmEntry, trackerURL string) (string, string, string, error) {
	swarmDir := filepath.Join(workDir, "swarm")
	if err := os.MkdirAll(swarmDir, 0o755); err != nil {
		return "", "", "", fmt.Errorf("mkdir swarm dir: %w", err)
	}

	payloadPath := sw.PayloadPath
	if payloadPath == "" {
		if sw.PayloadSize <= 0 {
			return "", "", "", fmt.Errorf("either payload_path or payload_size must be set")
		}
		// Hex suffix keeps multi-swarm scenarios distinguishable on
		// disk; the bytes inside are still freshly random per run so
		// the info_hash differs every invocation.
		suffix := make([]byte, 4)
		_, _ = rand.Read(suffix)
		payloadPath = filepath.Join(swarmDir, fmt.Sprintf("payload-%d-%s.bin", idx, hex.EncodeToString(suffix)))
		f, err := os.Create(payloadPath)
		if err != nil {
			return "", "", "", fmt.Errorf("create payload: %w", err)
		}
		if _, err := io.CopyN(f, rand.Reader, sw.PayloadSize); err != nil {
			f.Close()
			return "", "", "", fmt.Errorf("write random payload: %w", err)
		}
		f.Close()
	}

	res, err := torrentgen.GenerateFile(torrentgen.Spec{
		PayloadPath: payloadPath,
		PieceLength: sw.PieceLength,
		AnnounceURL: trackerURL,
	})
	if err != nil {
		return "", "", "", err
	}
	torrentPath := filepath.Join(swarmDir, fmt.Sprintf("%d.torrent", idx))
	if err := os.WriteFile(torrentPath, res.Torrent, 0o644); err != nil {
		return "", "", "", fmt.Errorf("write torrent: %w", err)
	}
	torrentName := filepath.Base(payloadPath)
	log.Printf("runner: swarm[%d] generated: payload=%s torrent=%s info_hash=%x size=%d",
		idx, payloadPath, torrentPath, res.InfoHash, res.Size)
	return torrentPath, payloadPath, torrentName, nil
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

// addOne is the per-(engine, torrent) add helper extracted so that the
// runner's Phase 2 loop can interleave seed-staging without duplicating
// the AddTorrent boilerplate.
func addOne(ctx context.Context, d engine.Driver, dataDir string, t scenario.TorrentEntry) error {
	meta, err := os.ReadFile(t.File)
	if err != nil {
		return fmt.Errorf("runner: read torrent %s: %w", t.File, err)
	}
	savePath := t.SavePath
	if !filepath.IsAbs(savePath) {
		savePath = filepath.Join(dataDir, savePath)
	}
	spec := engine.TorrentSpec{
		MetaBytes: meta,
		SavePath:  savePath,
	}
	addCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := d.AddTorrent(addCtx, spec); err != nil {
		return fmt.Errorf("runner: %s add_torrent %s: %w", d.Name(), t.File, err)
	}
	return nil
}

// copyFile streams src to dst. Used for seed staging where the payload
// can be larger than RAM. Truncates any existing file at dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
