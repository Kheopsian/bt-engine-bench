package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RqbitDriver wraps the rqbit BitTorrent engine. The engine is run inside a
// Docker container managed by the driver so that the bench remains
// reproducible regardless of what is installed on the host.
//
// Communication is HTTP-only — rqbit exposes a JSON API at
// http://<host>:<port>/. We use the global /stats endpoint for session
// aggregates rather than fanning out per-torrent calls; that endpoint was
// verified to exist on rqbit v8.x.
//
// The driver mounts the harness's data directory at /data inside the
// container and posts torrents as local file paths visible to that mount,
// so payload bytes do not have to round-trip through the HTTP API.
type RqbitDriver struct {
	// Image is the Docker image to use. Defaults to "ikatson/rqbit:latest".
	Image string

	// HostAPIPort is the port mapped from the container's HTTP API
	// (default 3030 inside the container). Caller MUST ensure no two
	// drivers share the same HostAPIPort within a run.
	HostAPIPort int

	// HostListenPort is the port mapped from the container's BitTorrent
	// listen port. rqbit defaults to 4240 inside the container; we keep
	// that fixed and only vary the host-side mapping.
	HostListenPort int

	container string
	httpc     *http.Client
	cfg       StartConfig
	baseURL   string
}

// NewRqbitDriver builds a driver with sensible defaults. Override fields
// before calling Start if the run needs custom values.
func NewRqbitDriver() *RqbitDriver {
	return &RqbitDriver{
		Image:          "ikatson/rqbit:latest",
		HostAPIPort:    3030,
		HostListenPort: 4240,
		httpc: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *RqbitDriver) Name() string { return "rqbit" }

func (d *RqbitDriver) Start(ctx context.Context, cfg StartConfig) error {
	d.cfg = cfg
	d.container = fmt.Sprintf("bench-rqbit-%d", time.Now().UnixNano())
	d.baseURL = fmt.Sprintf("http://127.0.0.1:%d", d.HostAPIPort)

	args := []string{
		"run", "-d",
		"--name", d.container,
		"-p", fmt.Sprintf("%d:3030", d.HostAPIPort),
		"-p", fmt.Sprintf("%d:4240", d.HostListenPort),
		"-v", fmt.Sprintf("%s:/data", cfg.DataDir),
		d.Image,
		"--http-api-listen-addr", "0.0.0.0:3030",
	}
	if cfg.DisableDHT {
		args = append(args, "--disable-dht")
	}
	args = append(args, "server", "start", "/data")

	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rqbit: docker run failed: %v: %s", err, out)
	}

	return d.waitReady(ctx, 30*time.Second)
}

// waitReady polls the rqbit API root until it responds with 2xx, or the
// timeout is reached.
func (d *RqbitDriver) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, "GET", d.baseURL+"/", nil)
		resp, err := d.httpc.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("rqbit: API did not become ready within %s", timeout)
}

func (d *RqbitDriver) AddTorrent(ctx context.Context, t TorrentSpec) error {
	// rqbit's POST /torrents accepts raw .torrent bytes when the body is
	// binary, OR a magnet/URL/path string. We post raw bytes for
	// simplicity and so the test harness does not have to materialise
	// a file on the shared volume just to add a torrent.
	url := d.baseURL + "/torrents"
	if t.SavePath != "" {
		// Output folder is overridable per-torrent via a query param.
		// Mapped through the container mount.
		rel, err := filepath.Rel(d.cfg.DataDir, t.SavePath)
		if err == nil && !strings.HasPrefix(rel, "..") {
			url += "?output_folder=" + filepath.Join("/data", rel)
		}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(t.MetaBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := d.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("rqbit: add_torrent: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rqbit: add_torrent http %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (d *RqbitDriver) StartTorrent(ctx context.Context, infoHash string) error {
	// rqbit auto-starts on add. Issue an explicit /start anyway in case a
	// future scenario adds in paused mode.
	url := fmt.Sprintf("%s/torrents/%s/start", d.baseURL, infoHash)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return err
	}
	resp, err := d.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("rqbit: start_torrent: %w", err)
	}
	resp.Body.Close()
	return nil
}

// rqbitGlobalStats mirrors the /stats response shape we care about. The
// real endpoint returns more fields (uptime, peer state buckets) — we
// pick only what maps cleanly into engine.Stats.
type rqbitGlobalStats struct {
	FetchedBytes  uint64 `json:"fetched_bytes"`
	UploadedBytes uint64 `json:"uploaded_bytes"`
	DownloadSpeed struct {
		Mbps float64 `json:"mbps"` // misnamed: actually MiB/s based on human_readable.
	} `json:"download_speed"`
	UploadSpeed struct {
		Mbps float64 `json:"mbps"`
	} `json:"upload_speed"`
	Peers struct {
		Live int `json:"live"`
	} `json:"peers"`
}

type rqbitTorrentList struct {
	Torrents []struct {
		ID       int    `json:"id"`
		InfoHash string `json:"info_hash"`
	} `json:"torrents"`
}

func (d *RqbitDriver) Stats(ctx context.Context) (Stats, error) {
	// 1. Global aggregates from /stats — single round-trip regardless of
	// how many torrents are loaded.
	greq, _ := http.NewRequestWithContext(ctx, "GET", d.baseURL+"/stats", nil)
	gresp, err := d.httpc.Do(greq)
	if err != nil {
		return Stats{}, fmt.Errorf("rqbit: GET /stats: %w", err)
	}
	defer gresp.Body.Close()
	var gs rqbitGlobalStats
	if err := json.NewDecoder(gresp.Body).Decode(&gs); err != nil {
		return Stats{}, fmt.Errorf("rqbit: decode /stats: %w", err)
	}

	// 2. Torrent count + active torrents — /stats does not break those
	// down so we still need /torrents (cheap: O(1) JSON list).
	lreq, _ := http.NewRequestWithContext(ctx, "GET", d.baseURL+"/torrents", nil)
	lresp, err := d.httpc.Do(lreq)
	if err != nil {
		return Stats{}, fmt.Errorf("rqbit: GET /torrents: %w", err)
	}
	defer lresp.Body.Close()
	var list rqbitTorrentList
	if err := json.NewDecoder(lresp.Body).Decode(&list); err != nil {
		return Stats{}, fmt.Errorf("rqbit: decode torrents list: %w", err)
	}

	const mibToBytes = 1024 * 1024
	return Stats{
		UploadRate:      uint64(gs.UploadSpeed.Mbps * mibToBytes),
		DownloadRate:    uint64(gs.DownloadSpeed.Mbps * mibToBytes),
		UploadedTotal:   gs.UploadedBytes,
		DownloadedTotal: gs.FetchedBytes,
		TorrentsTotal:   len(list.Torrents),
		// rqbit's /stats does not expose per-torrent activity. The
		// "active" notion is not directly observable here; downstream
		// scenarios that care about it can fan out to /torrents/{id}/stats/v1.
		TorrentsActive: 0,
		PeersConnected: gs.Peers.Live,
	}, nil
}

func (d *RqbitDriver) Stop(ctx context.Context) error {
	if d.container == "" {
		return nil
	}
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", d.container).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		return fmt.Errorf("rqbit: docker rm: %v: %s", err, out)
	}
	return nil
}
