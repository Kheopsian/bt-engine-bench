// Package metrics writes engine.Stats samples to a flat CSV ready for
// downstream analysis (Python pandas, sqlite import, etc.). The schema is
// intentionally stable: future drivers must fit it without breaking
// existing readers.
package metrics

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Kheopsian/bt-engine-bench/internal/engine"
)

// Writer appends one row per (engine, sample) tuple to a CSV file. Safe
// for concurrent calls — each driver runs its sample loop in its own
// goroutine, so the harness pumps writes from many goroutines.
type Writer struct {
	mu       sync.Mutex
	f        *os.File
	w        *csv.Writer
	scenario string
}

// columns defines the CSV schema. Order is load-bearing: every reader
// (plot scripts, BI tools) keys off positional columns.
var columns = []string{
	"ts_unix_ms",
	"scenario",
	"engine",
	"upload_rate_bps",
	"download_rate_bps",
	"uploaded_total_bytes",
	"downloaded_total_bytes",
	"torrents_total",
	"torrents_active",
	"peers_connected",
}

// New creates a fresh CSV file at path and writes the header. The caller
// owns the lifetime — Close() must be called to flush.
func New(path, scenarioName string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("metrics: create %s: %w", path, err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(columns); err != nil {
		f.Close()
		return nil, fmt.Errorf("metrics: write header: %w", err)
	}
	return &Writer{
		f:        f,
		w:        w,
		scenario: scenarioName,
	}, nil
}

// WriteSample appends a single sample row. Called from each driver's
// sample loop.
func (w *Writer) WriteSample(ts time.Time, engineName string, s engine.Stats) error {
	row := []string{
		strconv.FormatInt(ts.UnixMilli(), 10),
		w.scenario,
		engineName,
		strconv.FormatUint(s.UploadRate, 10),
		strconv.FormatUint(s.DownloadRate, 10),
		strconv.FormatUint(s.UploadedTotal, 10),
		strconv.FormatUint(s.DownloadedTotal, 10),
		strconv.Itoa(s.TorrentsTotal),
		strconv.Itoa(s.TorrentsActive),
		strconv.Itoa(s.PeersConnected),
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(row)
}

// Close flushes the buffered writer and closes the underlying file. Calling
// Close twice is a no-op.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	w.w.Flush()
	if err := w.w.Error(); err != nil {
		w.f.Close()
		w.f = nil
		return fmt.Errorf("metrics: flush: %w", err)
	}
	err := w.f.Close()
	w.f = nil
	return err
}
