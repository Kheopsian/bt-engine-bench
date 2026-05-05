package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// QbitDriver wraps qBittorrent (qbit-nox) which is the de-facto headless
// libtorrent reference implementation. The bench treats this driver as the
// stand-in for "libtorrent" in cross-engine comparisons.
//
// The driver pre-mounts a qBittorrent.conf that:
//   - accepts the legal notice (so the engine starts non-interactively);
//   - whitelists 0.0.0.0/0 for the WebUI auth bypass (no login flow needed);
//   - disables CSRF / Host-header validation so we can drive it from the
//     harness host without juggling cookies, Origin headers, or CSRF tokens.
//
// This is the right thing for a benchmark — none of those defenses are
// part of what we measure. Production qBit should keep them on.
type QbitDriver struct {
	// Image is the Docker image. Defaults to linuxserver/qbittorrent.
	Image string

	// HostPort is the host-side port mapped to container 8080 (WebUI).
	// Default 18080.
	HostPort int

	// HostListenPort is the host-side port mapped to the container's
	// BitTorrent listen port. Default 18881.
	HostListenPort int

	container string
	httpc     *http.Client
	cfg       StartConfig
	baseURL   string
	configDir string
}

// NewQbitDriver returns a driver pre-configured with sensible defaults.
func NewQbitDriver() *QbitDriver {
	return &QbitDriver{
		Image:          "linuxserver/qbittorrent:latest",
		HostPort:       18080,
		HostListenPort: 18881,
		httpc: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *QbitDriver) Name() string { return "libtorrent" }

// SeedPath implements engine.Seeder. qBittorrent rechecks files on add
// when it sees the data already in place, so we mirror the typhon
// convention: payload lands at savePath/<torrent_name>. SavePath in
// the qbit driver is mapped to /downloads/<rel> inside the container.
func (d *QbitDriver) SeedPath(savePath, torrentName string) string {
	return filepath.Join(savePath, torrentName)
}

func (d *QbitDriver) Start(ctx context.Context, cfg StartConfig) error {
	d.cfg = cfg
	d.container = fmt.Sprintf("bench-qbit-%d", time.Now().UnixNano())
	d.baseURL = fmt.Sprintf("http://127.0.0.1:%d", d.HostPort)

	// Generate the auth-bypass conf inside DataDir so it does not
	// outlive the run. linuxserver/qbittorrent expects the conf at
	// /config/qBittorrent/qBittorrent.conf — we mount DataDir/cfg
	// at /config so the engine sees it under that exact path.
	d.configDir = filepath.Join(cfg.DataDir, "cfg")
	confDir := filepath.Join(d.configDir, "qBittorrent")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		return fmt.Errorf("qbit: mkdir conf: %w", err)
	}
	// WebUI port = HostPort so each instance owns a distinct port on
	// the host network namespace. Other knobs are the same auth-bypass
	// trio that lets the harness drive qbit without juggling cookies.
	conf := []byte(fmt.Sprintf(`[LegalNotice]
Accepted=true

[BitTorrent]
Session\Port=%d

[Preferences]
WebUI\AuthSubnetWhitelistEnabled=true
WebUI\AuthSubnetWhitelist=0.0.0.0/0
WebUI\LocalHostAuth=false
WebUI\Port=%d
WebUI\Username=bench
WebUI\CSRFProtection=false
WebUI\HostHeaderValidation=false
`, cfg.ListenPort, d.HostPort))
	confPath := filepath.Join(confDir, "qBittorrent.conf")
	if err := os.WriteFile(confPath, conf, 0o644); err != nil {
		return fmt.Errorf("qbit: write conf: %w", err)
	}

	// Downloads dir (separate from conf so cfg dir is writable but
	// not flooded with payload).
	downloadsDir := filepath.Join(cfg.DataDir, "downloads")
	if err := os.MkdirAll(downloadsDir, 0o755); err != nil {
		return fmt.Errorf("qbit: mkdir downloads: %w", err)
	}

	// --network host: same rationale as rqbit/rtorrent. WebUI and BT
	// listen ports come from the conf above so each engine instance
	// owns its own slice of host ports.
	args := []string{
		"run", "-d",
		"--name", d.container,
		"--network", "host",
		"-v", fmt.Sprintf("%s:/config", d.configDir),
		"-v", fmt.Sprintf("%s:/downloads", downloadsDir),
		"-e", "PUID=1000",
		"-e", "PGID=1000",
		"-e", fmt.Sprintf("WEBUI_PORT=%d", d.HostPort),
		d.Image,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("qbit: docker run failed: %v: %s", err, out)
	}

	return d.waitReady(ctx, 60*time.Second)
}

// waitReady polls /api/v2/transfer/info until it responds 200.
func (d *QbitDriver) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, "GET", d.baseURL+"/api/v2/transfer/info", nil)
		resp, err := d.httpc.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("qbit: WebUI did not become ready within %s", timeout)
}

func (d *QbitDriver) AddTorrent(ctx context.Context, t TorrentSpec) error {
	if t.SavePath == "" {
		return fmt.Errorf("qbit: TorrentSpec.SavePath is required")
	}

	// Multipart with the .torrent file under field name "torrents" and
	// the savepath as a regular form field. qBit expects the savepath
	// to point at a directory the daemon can write to inside its own
	// filesystem view (i.e. /downloads/... after the volume mount).
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	relSave, err := filepath.Rel(d.cfg.DataDir, t.SavePath)
	if err != nil || strings.HasPrefix(relSave, "..") {
		return fmt.Errorf("qbit: SavePath must live under DataDir: %q", t.SavePath)
	}
	containerSavePath := filepath.Join("/downloads", strings.TrimPrefix(relSave, "downloads/"))
	if err := mw.WriteField("savepath", containerSavePath); err != nil {
		return err
	}
	// "Add torrents in stopped state? false" — we want them live.
	if err := mw.WriteField("paused", "false"); err != nil {
		return err
	}
	// Forward Seed flag → skip qBit's hash check on add. With pre-staged
	// payload (seeders in scenario), the leecher pair would otherwise
	// wait the full verify duration (~10 MB/s, ~7 min on a 4 GB file)
	// before the engine becomes a peer-visible seeder. qBit's WebUI param
	// for this is `skip_checking=true`.
	if t.Seed {
		if err := mw.WriteField("skip_checking", "true"); err != nil {
			return err
		}
	}

	// File part. Use a stable filename so qBit's logs are grep-able.
	hdr := make(textproto.MIMEHeader)
	hdr.Set("Content-Disposition", `form-data; name="torrents"; filename="bench.torrent"`)
	hdr.Set("Content-Type", "application/x-bittorrent")
	fw, err := mw.CreatePart(hdr)
	if err != nil {
		return err
	}
	if _, err := fw.Write(t.MetaBytes); err != nil {
		return err
	}
	mw.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", d.baseURL+"/api/v2/torrents/add", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := d.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("qbit: add: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		out, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qbit: add http %d: %s", resp.StatusCode, out)
	}
	return nil
}

func (d *QbitDriver) StartTorrent(ctx context.Context, infoHash string) error {
	// We added with paused=false, so this is a no-op for the bench
	// happy path. Issue /api/v2/torrents/start for completeness.
	body := strings.NewReader("hashes=" + infoHash)
	req, err := http.NewRequestWithContext(ctx, "POST", d.baseURL+"/api/v2/torrents/start", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := d.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("qbit: start: %w", err)
	}
	resp.Body.Close()
	return nil
}

type qbitTransferInfo struct {
	DlSpeed         uint64 `json:"dl_info_speed"`
	UlSpeed         uint64 `json:"up_info_speed"`
	DlData          uint64 `json:"dl_info_data"`
	UlData          uint64 `json:"up_info_data"`
}

type qbitMaindata struct {
	Torrents map[string]struct {
		State    string  `json:"state"`
		NumLeechs int    `json:"num_leechs"`
		NumSeeds  int    `json:"num_seeds"`
		UpSpeed  uint64  `json:"upspeed"`
		DlSpeed  uint64  `json:"dlspeed"`
	} `json:"torrents"`
	ServerState struct {
		TotalPeerConnections int `json:"total_peer_connections"`
	} `json:"server_state"`
}

func (d *QbitDriver) Stats(ctx context.Context) (Stats, error) {
	// Rates + cumulative totals from /transfer/info — single call.
	req1, _ := http.NewRequestWithContext(ctx, "GET", d.baseURL+"/api/v2/transfer/info", nil)
	r1, err := d.httpc.Do(req1)
	if err != nil {
		return Stats{}, fmt.Errorf("qbit: transfer/info: %w", err)
	}
	defer r1.Body.Close()
	var ti qbitTransferInfo
	if err := json.NewDecoder(r1.Body).Decode(&ti); err != nil {
		return Stats{}, fmt.Errorf("qbit: decode transfer/info: %w", err)
	}

	// Torrent counts and global peer connections from /sync/maindata.
	// We could cache the rid-based delta protocol for very large lists,
	// but for the bench's typical scenario sizes a fresh rid=0 query is
	// cheap and avoids client-side state we'd have to drive carefully.
	req2, _ := http.NewRequestWithContext(ctx, "GET", d.baseURL+"/api/v2/sync/maindata?rid=0", nil)
	r2, err := d.httpc.Do(req2)
	if err != nil {
		return Stats{}, fmt.Errorf("qbit: sync/maindata: %w", err)
	}
	defer r2.Body.Close()
	var md qbitMaindata
	if err := json.NewDecoder(r2.Body).Decode(&md); err != nil {
		return Stats{}, fmt.Errorf("qbit: decode maindata: %w", err)
	}

	var active int
	for _, t := range md.Torrents {
		if t.UpSpeed > 0 || t.DlSpeed > 0 || t.NumLeechs+t.NumSeeds > 0 {
			active++
		}
	}

	return Stats{
		UploadRate:      ti.UlSpeed,
		DownloadRate:    ti.DlSpeed,
		UploadedTotal:   ti.UlData,
		DownloadedTotal: ti.DlData,
		TorrentsTotal:   len(md.Torrents),
		TorrentsActive:  active,
		PeersConnected:  md.ServerState.TotalPeerConnections,
	}, nil
}

func (d *QbitDriver) Stop(ctx context.Context) error {
	if d.container == "" {
		return nil
	}
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", d.container).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		return fmt.Errorf("qbit: docker rm: %v: %s", err, out)
	}
	return nil
}
