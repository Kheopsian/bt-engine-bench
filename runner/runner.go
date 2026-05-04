// Package runner orchestrates a single scenario across one or more engine
// drivers. Each engine runs in isolation (its own data dir, its own
// containerised process) but all of them are fed the same torrent set on
// the same timeline.
package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Kheopsian/bt-engine-bench/internal/engine"
	"github.com/Kheopsian/bt-engine-bench/internal/metrics"
	"github.com/Kheopsian/bt-engine-bench/internal/scenario"
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
	for _, r := range live {
		for _, t := range sc.Torrents {
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
