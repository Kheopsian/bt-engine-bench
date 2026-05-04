package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"
)

// RainDriver wraps the cenkalti/rain BitTorrent engine. Rain is a
// pure-Go engine famous for its 1-goroutine-per-peer model — distinct
// implementation strategy from rqbit (libtorrent-style state machine)
// and qbit-nox (libtorrent C++ via Boost.Asio). Including it gives the
// bench a real diversity of designs to compare.
//
// Rain has no public Docker image, so the driver expects a path to the
// rain binary on the host — same pattern as typhon. Build it from
// https://github.com/cenkalti/rain or grab a release artefact.
//
// Wire protocol: standard JSON-RPC 2.0 over HTTP (powerman/rpc-codec
// flavour). Service registered as "Session"; methods are
// `Session.<MethodName>`.
//
// Rain limitation surfaced through this driver: AddTorrentOptions has
// no SavePath field. Every torrent lands in the global config DataDir.
// The bench's per-engine workdir already isolates that, so it is fine.
type RainDriver struct {
	// BinaryPath is the path to the rain binary. Required.
	BinaryPath string

	// HostRPCPort is the port rain's RPC server should listen on.
	// Default 17246.
	HostRPCPort int

	cmd     *exec.Cmd
	cfg     StartConfig
	httpc   *http.Client
	baseURL string
	idSeq   atomic.Int64
}

func NewRainDriver(binaryPath string) *RainDriver {
	return &RainDriver{
		BinaryPath:  binaryPath,
		HostRPCPort: 17246,
		httpc: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *RainDriver) Name() string { return "rain" }

func (d *RainDriver) Start(ctx context.Context, cfg StartConfig) error {
	if d.BinaryPath == "" {
		return errors.New("rain: BinaryPath is required")
	}
	if _, err := os.Stat(d.BinaryPath); err != nil {
		return fmt.Errorf("rain: binary not found at %s: %w", d.BinaryPath, err)
	}
	d.cfg = cfg
	d.baseURL = fmt.Sprintf("http://127.0.0.1:%d/", d.HostRPCPort)

	// Rain's YAML config uses field names lowercased without underscores
	// (RPCPort → rpcport, DataDir → datadir, …). Anything misspelt is
	// silently ignored, so be careful when adding knobs here.
	confPath := filepath.Join(cfg.DataDir, "rain.yaml")
	conf := fmt.Sprintf(`datadir: %s
datadirincludestorrentid: false
rpchost: 0.0.0.0
rpcport: %d
portbegin: %d
portend: %d
parallelreads: 16
resumeonstartup: false
dhtenabled: %t
peexenabled: %t
`,
		cfg.DataDir,
		d.HostRPCPort,
		cfg.ListenPort, cfg.ListenPort+10,
		!cfg.DisableDHT,
		!cfg.DisablePEX,
	)
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("rain: mkdir data dir: %w", err)
	}
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		return fmt.Errorf("rain: write config: %w", err)
	}

	// Bind the engine process to the driver's own lifetime, not to
	// Start's startup-deadline ctx. Same rationale as typhon: a child
	// of a CommandContext gets SIGKILL the instant the caller cancels
	// its startup-timeout context.
	d.cmd = exec.Command(d.BinaryPath, "server", "-c", confPath)
	d.cmd.Stdout = os.Stdout
	d.cmd.Stderr = os.Stderr
	if err := d.cmd.Start(); err != nil {
		return fmt.Errorf("rain: start process: %w", err)
	}
	return d.waitReady(ctx, 30*time.Second)
}

func (d *RainDriver) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := d.call(ctx, "Session.GetSessionStats", []interface{}{struct{}{}}); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("rain: RPC not ready within %s", timeout)
}

// call sends a JSON-RPC 2.0 request and returns the raw `result` field on
// success or a Go error mirroring the JSON-RPC error.
func (d *RainDriver) call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	id := d.idSeq.Add(1)
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", d.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rain: rpc post: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("rain: read body: %w", err)
	}
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("rain: decode envelope: %w (body: %s)", err, raw)
	}
	if env.Error != nil {
		return nil, fmt.Errorf("rain: rpc error %d: %s", env.Error.Code, env.Error.Message)
	}
	return env.Result, nil
}

func (d *RainDriver) AddTorrent(ctx context.Context, t TorrentSpec) error {
	// Rain ignores per-torrent save paths — payload always lands in
	// the engine's global DataDir. We accept t.SavePath silently;
	// downstream tooling that compares directories needs to look
	// under DataDir.
	_, err := d.call(ctx, "Session.AddTorrent", []interface{}{
		map[string]interface{}{
			"Torrent": base64.StdEncoding.EncodeToString(t.MetaBytes),
			"Stopped": false,
		},
	})
	return err
}

func (d *RainDriver) StartTorrent(ctx context.Context, infoHash string) error {
	_, err := d.call(ctx, "Session.StartTorrent", []interface{}{
		map[string]interface{}{"ID": infoHash},
	})
	return err
}

// rainSessionStats mirrors the SessionStats struct in
// rain/internal/rpctypes — only the fields the bench exposes are decoded.
type rainSessionStatsResp struct {
	Stats struct {
		Torrents        int   `json:"Torrents"`
		Peers           int   `json:"Peers"`
		SpeedDownload   int   `json:"SpeedDownload"`
		SpeedUpload     int   `json:"SpeedUpload"`
		BytesDownloaded int64 `json:"BytesDownloaded"`
		BytesUploaded   int64 `json:"BytesUploaded"`
	} `json:"Stats"`
}

func (d *RainDriver) Stats(ctx context.Context) (Stats, error) {
	raw, err := d.call(ctx, "Session.GetSessionStats", []interface{}{struct{}{}})
	if err != nil {
		return Stats{}, err
	}
	var resp rainSessionStatsResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		return Stats{}, fmt.Errorf("rain: decode session stats: %w", err)
	}
	return Stats{
		UploadRate:      uint64(resp.Stats.SpeedUpload),
		DownloadRate:    uint64(resp.Stats.SpeedDownload),
		UploadedTotal:   uint64(resp.Stats.BytesUploaded),
		DownloadedTotal: uint64(resp.Stats.BytesDownloaded),
		TorrentsTotal:   resp.Stats.Torrents,
		// Rain does not surface "active" vs "all" at session scope.
		TorrentsActive: 0,
		PeersConnected: resp.Stats.Peers,
	}, nil
}

func (d *RainDriver) Stop(ctx context.Context) error {
	if d.cmd == nil || d.cmd.Process == nil {
		return nil
	}
	_ = d.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- d.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = d.cmd.Process.Kill()
		<-done
	case <-ctx.Done():
		_ = d.cmd.Process.Kill()
		<-done
	}
	return nil
}
