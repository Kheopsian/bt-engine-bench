package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// TransmissionDriver wraps the Transmission BitTorrent engine. The engine is
// run inside a Docker container managed by the driver. Communication is via
// the JSON-RPC at /transmission/rpc on port 9091.
//
// Quirks:
//   - Transmission uses an X-Transmission-Session-Id CSRF token: the FIRST
//     request always responds 409 with the new token in a header. The
//     driver retries once after observing 409.
//   - session-stats returns aggregate counters (bytes, speeds, torrent
//     counts) but not connected-peer count; to expose that we fan out
//     to torrent-get with field "peersConnected".
//
// The driver assumes the host has nothing else listening on the configured
// HostPort / HostListenPort.
type TransmissionDriver struct {
	// Image is the Docker image. Defaults to linuxserver/transmission.
	Image string

	// HostPort is the host-side port mapped to container port 9091
	// (RPC). Default 19091.
	HostPort int

	// HostListenPort is the host-side port mapped to container port
	// 51413 (BitTorrent listen). Default 19092.
	HostListenPort int

	container string
	httpc     *http.Client
	cfg       StartConfig
	baseURL   string

	sidMu sync.Mutex
	sid   string
}

// NewTransmissionDriver returns a driver pre-configured with sensible
// defaults; override fields before calling Start.
func NewTransmissionDriver() *TransmissionDriver {
	return &TransmissionDriver{
		Image:          "linuxserver/transmission:latest",
		HostPort:       19091,
		HostListenPort: 19092,
		httpc: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *TransmissionDriver) Name() string { return "transmission" }

func (d *TransmissionDriver) Start(ctx context.Context, cfg StartConfig) error {
	d.cfg = cfg
	d.container = fmt.Sprintf("bench-tr-%d", time.Now().UnixNano())
	d.baseURL = fmt.Sprintf("http://127.0.0.1:%d/transmission/rpc", d.HostPort)

	args := []string{
		"run", "-d",
		"--name", d.container,
		"-p", fmt.Sprintf("%d:9091", d.HostPort),
		"-p", fmt.Sprintf("%d:%d", d.HostListenPort, cfg.ListenPort),
		"-p", fmt.Sprintf("%d:%d/udp", d.HostListenPort, cfg.ListenPort),
		"-v", fmt.Sprintf("%s:/downloads", cfg.DataDir),
		"-e", "TRANSMISSION_WEB_HOME=/transmission-web-control",
		"-e", fmt.Sprintf("PEERPORT=%d", cfg.ListenPort),
		d.Image,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("transmission: docker run failed: %v: %s", err, out)
	}

	// Transmission's container takes a while to bootstrap (creates
	// settings.json, starts the daemon, opens the RPC port). 60s is a
	// safe upper bound from the linuxserver image; tune down if your
	// scenario times out before that.
	return d.waitReady(ctx, 60*time.Second)
}

func (d *TransmissionDriver) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := d.refreshSessionID(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("transmission: RPC did not become ready within %s", timeout)
}

// refreshSessionID issues a no-op POST that we expect to 409 with the
// current X-Transmission-Session-Id header. We capture the token for
// subsequent requests.
func (d *TransmissionDriver) refreshSessionID(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "POST", d.baseURL,
		bytes.NewReader([]byte(`{"method":"session-stats"}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sid := resp.Header.Get("X-Transmission-Session-Id")
	if sid == "" {
		return fmt.Errorf("transmission: no session id in response (status %d)", resp.StatusCode)
	}
	d.sidMu.Lock()
	d.sid = sid
	d.sidMu.Unlock()
	return nil
}

// rpc sends a JSON-RPC request, transparently refreshing the session id on
// 409 and retrying once.
func (d *TransmissionDriver) rpc(ctx context.Context, body []byte) ([]byte, error) {
	for retry := 0; retry < 2; retry++ {
		req, err := http.NewRequestWithContext(ctx, "POST", d.baseURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		d.sidMu.Lock()
		req.Header.Set("X-Transmission-Session-Id", d.sid)
		d.sidMu.Unlock()
		resp, err := d.httpc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("transmission: rpc: %w", err)
		}
		if resp.StatusCode == http.StatusConflict {
			sid := resp.Header.Get("X-Transmission-Session-Id")
			resp.Body.Close()
			if sid == "" {
				return nil, fmt.Errorf("transmission: 409 without session id")
			}
			d.sidMu.Lock()
			d.sid = sid
			d.sidMu.Unlock()
			continue // retry once with fresh sid
		}
		out, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("transmission: read body: %w", err)
		}
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("transmission: http %d: %s", resp.StatusCode, out)
		}
		return out, nil
	}
	return nil, fmt.Errorf("transmission: gave up after session-id refresh")
}

func (d *TransmissionDriver) AddTorrent(ctx context.Context, t TorrentSpec) error {
	if t.SavePath == "" {
		return fmt.Errorf("transmission: TorrentSpec.SavePath is required")
	}
	body, _ := json.Marshal(map[string]interface{}{
		"method": "torrent-add",
		"arguments": map[string]interface{}{
			"metainfo":     base64.StdEncoding.EncodeToString(t.MetaBytes),
			"download-dir": t.SavePath,
			"paused":       false,
		},
	})
	out, err := d.rpc(ctx, body)
	if err != nil {
		return err
	}
	var probe struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		return fmt.Errorf("transmission: decode torrent-add: %w", err)
	}
	if probe.Result != "success" {
		return fmt.Errorf("transmission: torrent-add failed: %s", probe.Result)
	}
	return nil
}

func (d *TransmissionDriver) StartTorrent(ctx context.Context, infoHash string) error {
	// torrent-add already starts paused=false; this is a no-op when
	// the caller already added the torrent. We still issue torrent-start
	// so a paused-on-add scenario can transition cleanly.
	body, _ := json.Marshal(map[string]interface{}{
		"method": "torrent-start",
		"arguments": map[string]interface{}{
			"ids": []string{infoHash},
		},
	})
	_, err := d.rpc(ctx, body)
	return err
}

type trSessionStats struct {
	Result    string `json:"result"`
	Arguments struct {
		ActiveTorrentCount int   `json:"activeTorrentCount"`
		TorrentCount       int   `json:"torrentCount"`
		DownloadSpeed      int64 `json:"downloadSpeed"`
		UploadSpeed        int64 `json:"uploadSpeed"`
		CumulativeStats    struct {
			DownloadedBytes int64 `json:"downloadedBytes"`
			UploadedBytes   int64 `json:"uploadedBytes"`
		} `json:"cumulative-stats"`
	} `json:"arguments"`
}

type trTorrentGet struct {
	Result    string `json:"result"`
	Arguments struct {
		Torrents []struct {
			PeersConnected int `json:"peersConnected"`
		} `json:"torrents"`
	} `json:"arguments"`
}

func (d *TransmissionDriver) Stats(ctx context.Context) (Stats, error) {
	// Aggregate counters via session-stats.
	body, _ := json.Marshal(map[string]string{"method": "session-stats"})
	raw, err := d.rpc(ctx, body)
	if err != nil {
		return Stats{}, err
	}
	var ss trSessionStats
	if err := json.Unmarshal(raw, &ss); err != nil {
		return Stats{}, fmt.Errorf("transmission: decode session-stats: %w", err)
	}

	// Peers connected — not in session-stats. Fan out to torrent-get
	// with only the "peersConnected" field; transmission returns an
	// array even for sessions with no torrents.
	var peers int
	body, _ = json.Marshal(map[string]interface{}{
		"method": "torrent-get",
		"arguments": map[string]interface{}{
			"fields": []string{"peersConnected"},
		},
	})
	if raw, err := d.rpc(ctx, body); err == nil {
		var tg trTorrentGet
		if err := json.Unmarshal(raw, &tg); err == nil {
			for _, t := range tg.Arguments.Torrents {
				peers += t.PeersConnected
			}
		}
	}

	return Stats{
		UploadRate:      uint64(ss.Arguments.UploadSpeed),
		DownloadRate:    uint64(ss.Arguments.DownloadSpeed),
		UploadedTotal:   uint64(ss.Arguments.CumulativeStats.UploadedBytes),
		DownloadedTotal: uint64(ss.Arguments.CumulativeStats.DownloadedBytes),
		TorrentsTotal:   ss.Arguments.TorrentCount,
		TorrentsActive:  ss.Arguments.ActiveTorrentCount,
		PeersConnected:  peers,
	}, nil
}

func (d *TransmissionDriver) Stop(ctx context.Context) error {
	if d.container == "" {
		return nil
	}
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", d.container).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		return fmt.Errorf("transmission: docker rm: %v: %s", err, out)
	}
	return nil
}