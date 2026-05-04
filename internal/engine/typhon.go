package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// TyphonDriver wraps the typhon BitTorrent engine. typhon is a standalone
// Rust binary (`hydra-engine`) that listens on a Unix socket and accepts
// newline-delimited JSON-RPC requests.
//
// Unlike rqbit, typhon does not ship as a public Docker image. The driver
// therefore expects the caller to supply a path to the binary; the binary
// can be obtained either by building from the typhon-engine source tree
// or by extracting `/usr/local/bin/hydra-engine` from a hydra-go image.
type TyphonDriver struct {
	// BinaryPath is the absolute path to the hydra-engine binary on
	// the host running the harness. Required.
	BinaryPath string

	// SocketDir is the directory in which the driver creates the Unix
	// socket and the engine config file. Defaults to a per-run subdir
	// under StartConfig.DataDir.
	SocketDir string

	cmd      *exec.Cmd
	conn     net.Conn
	socket   string
	cfg      StartConfig

	// JSON-RPC plumbing: idSeq generates monotonic request ids; the
	// reader goroutine demultiplexes responses by id.
	idSeq    atomic.Int64
	pendingM sync.Mutex
	pending  map[int64]chan json.RawMessage
	writeM   sync.Mutex
	closed   atomic.Bool
}

// NewTyphonDriver builds a driver pointing at the given binary path.
func NewTyphonDriver(binaryPath string) *TyphonDriver {
	return &TyphonDriver{
		BinaryPath: binaryPath,
		pending:    make(map[int64]chan json.RawMessage),
	}
}

func (d *TyphonDriver) Name() string { return "typhon" }

// typhonConfig is the JSON document hydra-engine reads from --config. We
// expose only the knobs a benchmark scenario actually controls; everything
// else picks up engine defaults.
type typhonConfig struct {
	DataDir              string `json:"data_dir"`
	ResumeDir            string `json:"resume_dir"`
	ListenPort           int    `json:"listen_port"`
	MaxConnections       int    `json:"max_connections,omitempty"`
	MaxConnsPerTorrent   int    `json:"max_connections_per_torrent,omitempty"`
	EnableDHT            bool   `json:"enable_dht"`
	EnablePEX            bool   `json:"enable_pex"`
	EnableLSD            bool   `json:"enable_lsd"`
}

func (d *TyphonDriver) Start(ctx context.Context, cfg StartConfig) error {
	if d.BinaryPath == "" {
		return errors.New("typhon: BinaryPath is required")
	}
	if _, err := os.Stat(d.BinaryPath); err != nil {
		return fmt.Errorf("typhon: binary not found at %s: %w", d.BinaryPath, err)
	}
	d.cfg = cfg

	// Place sockets and config under DataDir to keep the run hermetic
	// (one harness can drive multiple typhon instances by varying DataDir).
	if d.SocketDir == "" {
		d.SocketDir = cfg.DataDir
	}
	if err := os.MkdirAll(d.SocketDir, 0o755); err != nil {
		return fmt.Errorf("typhon: mkdir socket dir: %w", err)
	}
	resumeDir := filepath.Join(cfg.DataDir, "resume")
	if err := os.MkdirAll(resumeDir, 0o755); err != nil {
		return fmt.Errorf("typhon: mkdir resume dir: %w", err)
	}

	d.socket = filepath.Join(d.SocketDir, "typhon.sock")
	configPath := filepath.Join(d.SocketDir, "typhon.config.json")

	conf := typhonConfig{
		DataDir:            cfg.DataDir,
		ResumeDir:          resumeDir,
		ListenPort:         cfg.ListenPort,
		MaxConnections:     cfg.MaxTotalConnections,
		MaxConnsPerTorrent: cfg.MaxPeersPerTorrent,
		EnableDHT:          !cfg.DisableDHT,
		EnablePEX:          !cfg.DisablePEX,
		EnableLSD:          !cfg.DisableLSD,
	}
	confBytes, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		return fmt.Errorf("typhon: marshal config: %w", err)
	}
	if err := os.WriteFile(configPath, confBytes, 0o644); err != nil {
		return fmt.Errorf("typhon: write config: %w", err)
	}

	// Spawn the engine. Stdout/Stderr are forwarded so the harness's
	// log captures any panic from the engine side directly.
	d.cmd = exec.CommandContext(ctx, d.BinaryPath, "--config", configPath, "--socket", d.socket)
	d.cmd.Stdout = os.Stdout
	d.cmd.Stderr = os.Stderr
	if err := d.cmd.Start(); err != nil {
		return fmt.Errorf("typhon: start engine process: %w", err)
	}

	// Connect to the Unix socket once the engine has bound it. typhon
	// usually binds within ~50ms but cold-load with a populated resume
	// dir can stretch much longer; we give it 30s, same as rqbit.
	if err := d.connect(ctx, 30*time.Second); err != nil {
		_ = d.cmd.Process.Kill()
		return err
	}

	go d.readLoop()

	// Sanity ping. If the engine binds the socket but the RPC dispatch
	// is not yet ready we want to surface that as a Start error rather
	// than a confusing AddTorrent failure later.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := d.call(pingCtx, "ping", struct{}{}); err != nil {
		return fmt.Errorf("typhon: post-connect ping failed: %w", err)
	}
	return nil
}

func (d *TyphonDriver) connect(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("unix", d.socket, 1*time.Second)
		if err == nil {
			d.conn = c
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("typhon: socket %s did not become ready within %s", d.socket, timeout)
}

// readLoop consumes newline-delimited JSON frames from the engine and routes
// each response to the goroutine that issued the matching request.
func (d *TyphonDriver) readLoop() {
	br := bufio.NewReader(d.conn)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			d.closed.Store(true)
			d.failPending(err)
			return
		}
		var resp struct {
			ID    int64           `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  string          `json:"error"`
		}
		if jerr := json.Unmarshal(line, &resp); jerr != nil {
			continue // ignore malformed frames; engine may emit
			// out-of-band events someday.
		}
		d.pendingM.Lock()
		ch, ok := d.pending[resp.ID]
		delete(d.pending, resp.ID)
		d.pendingM.Unlock()
		if !ok {
			continue // orphan response, request goroutine timed out.
		}
		if resp.Error != "" {
			ch <- json.RawMessage(`{"__rpc_error":"` + resp.Error + `"}`)
		} else {
			ch <- resp.Result
		}
	}
}

func (d *TyphonDriver) failPending(err error) {
	d.pendingM.Lock()
	for id, ch := range d.pending {
		ch <- json.RawMessage(`{"__rpc_error":"` + err.Error() + `"}`)
		delete(d.pending, id)
	}
	d.pendingM.Unlock()
}

// call sends a single JSON-RPC request and blocks until the response arrives
// or ctx expires.
func (d *TyphonDriver) call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	if d.closed.Load() {
		return nil, errors.New("typhon: client closed")
	}
	id := d.idSeq.Add(1)
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("typhon: marshal params: %w", err)
	}
	frame, err := json.Marshal(struct {
		ID     int64           `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}{ID: id, Method: method, Params: paramsJSON})
	if err != nil {
		return nil, err
	}
	frame = append(frame, '\n')

	ch := make(chan json.RawMessage, 1)
	d.pendingM.Lock()
	d.pending[id] = ch
	d.pendingM.Unlock()

	d.writeM.Lock()
	_, werr := d.conn.Write(frame)
	d.writeM.Unlock()
	if werr != nil {
		d.pendingM.Lock()
		delete(d.pending, id)
		d.pendingM.Unlock()
		return nil, fmt.Errorf("typhon: write rpc: %w", werr)
	}

	select {
	case raw := <-ch:
		var probe struct {
			Err string `json:"__rpc_error"`
		}
		if json.Unmarshal(raw, &probe) == nil && probe.Err != "" {
			return nil, errors.New("typhon: " + probe.Err)
		}
		return raw, nil
	case <-ctx.Done():
		d.pendingM.Lock()
		delete(d.pending, id)
		d.pendingM.Unlock()
		return nil, ctx.Err()
	}
}

// resultErr is the typhon convention for application-level errors: the RPC
// envelope succeeds but the result body itself contains {"error": "..."}.
// Every caller has to peel that off before trusting the result.
type resultErr struct {
	Error string `json:"error"`
}

func (d *TyphonDriver) AddTorrent(ctx context.Context, t TorrentSpec) error {
	if t.SavePath == "" {
		return errors.New("typhon: TorrentSpec.SavePath is required")
	}
	// typhon reads the torrent file from disk, so we materialise it.
	torrentPath := filepath.Join(d.cfg.DataDir, t.InfoHash+".torrent")
	if err := os.WriteFile(torrentPath, t.MetaBytes, 0o644); err != nil {
		return fmt.Errorf("typhon: write meta: %w", err)
	}
	if err := os.MkdirAll(t.SavePath, 0o755); err != nil {
		return fmt.Errorf("typhon: mkdir save_path: %w", err)
	}
	raw, err := d.call(ctx, "add_torrent", map[string]interface{}{
		"torrent_path": torrentPath,
		"save_path":    t.SavePath,
		"stopped":      false,
	})
	if err != nil {
		return err
	}
	var probe resultErr
	if json.Unmarshal(raw, &probe) == nil && probe.Error != "" {
		return errors.New("typhon: add_torrent: " + probe.Error)
	}
	return nil
}

func (d *TyphonDriver) StartTorrent(ctx context.Context, infoHash string) error {
	raw, err := d.call(ctx, "start_torrent", map[string]string{"info_hash": infoHash})
	if err != nil {
		return err
	}
	var probe resultErr
	if json.Unmarshal(raw, &probe) == nil && probe.Error != "" {
		return errors.New("typhon: start_torrent: " + probe.Error)
	}
	return nil
}

// typhonSessionStats mirrors get_session_stats as actually emitted by the
// dispatch.rs in typhon-engine v2.5.x. typhon does not surface per-torrent
// active/idle counts at the session level, so TorrentsActive stays zero.
type typhonSessionStats struct {
	UploadRate      uint64 `json:"upload_rate"`
	DownloadRate    uint64 `json:"download_rate"`
	TotalUpload     uint64 `json:"total_upload"`
	TotalDownload   uint64 `json:"total_download"`
	NumTorrents     int    `json:"num_torrents"`
	UnseededPeers   int    `json:"unseeded_peers"`
}

func (d *TyphonDriver) Stats(ctx context.Context) (Stats, error) {
	raw, err := d.call(ctx, "get_session_stats", struct{}{})
	if err != nil {
		return Stats{}, err
	}
	var s typhonSessionStats
	if err := json.Unmarshal(raw, &s); err != nil {
		return Stats{}, fmt.Errorf("typhon: decode session stats: %w", err)
	}
	return Stats{
		UploadRate:      s.UploadRate,
		DownloadRate:    s.DownloadRate,
		UploadedTotal:   s.TotalUpload,
		DownloadedTotal: s.TotalDownload,
		TorrentsTotal:   s.NumTorrents,
		TorrentsActive:  0, // not exposed by typhon at session scope.
		PeersConnected:  s.UnseededPeers,
	}, nil
}

func (d *TyphonDriver) Stop(ctx context.Context) error {
	d.closed.Store(true)
	if d.conn != nil {
		_ = d.conn.Close()
	}
	if d.cmd != nil && d.cmd.Process != nil {
		// SIGTERM first; the engine's own graceful shutdown writes
		// fastresume data and closes peer connections. Force-kill
		// after a grace window so a hung engine cannot pin the
		// harness forever.
		_ = d.cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- d.cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = d.cmd.Process.Kill()
			<-done
		case <-ctx.Done():
			_ = d.cmd.Process.Kill()
			<-done
		}
	}
	return nil
}
