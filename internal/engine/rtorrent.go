package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// RtorrentDriver wraps the rtorrent BitTorrent engine. rtorrent is the
// "third major C-family BT engine" not based on libtorrent — distinct
// codebase used by countless seedboxes via ruTorrent. Including it makes
// the bench reflect a meaningful chunk of the public-facing client zoo
// that libtorrent and the Rust newcomers never touch.
//
// Quirks:
//   - rtorrent's RPC is XML-RPC over SCGI (binary length-prefixed
//     headers, then XML body). Not the most amenable to Go but trivial
//     in 50 lines.
//   - The default jesec/rtorrent image already enables SCGI on port
//     5000, so the driver does not pass `network.scgi.open_port` again
//     (rtorrent errors out on duplicate enable).
//   - Headless mode requires `system.daemon.set=true`. Without it the
//     binary tries to open a curses interface and exits.
//   - rtorrent has no single "session-stats" RPC; the driver fans out
//     a small system.multicall to fetch the four global throttle
//     counters in one round trip.
type RtorrentDriver struct {
	// Image is the Docker image. Defaults to jesec/rtorrent:latest.
	Image string

	// HostSCGIPort is the host-side port mapped to container's SCGI
	// listener (5000). Default 15000.
	HostSCGIPort int

	// HostListenPort is the host-side port mapped to rtorrent's
	// BitTorrent listen. Default 15010.
	HostListenPort int

	container string
	cfg       StartConfig
	scgiAddr  string
}

func NewRtorrentDriver() *RtorrentDriver {
	return &RtorrentDriver{
		Image:          "jesec/rtorrent:latest",
		HostSCGIPort:   15000,
		HostListenPort: 15010,
	}
}

func (d *RtorrentDriver) Name() string { return "rtorrent" }

// SeedPath implements engine.Seeder. With our `directory.default.set=
// /rtorrent/data` config and the host volume mount /rtorrent → DataDir,
// the host path that the engine reads as /rtorrent/data/<name> is
// DataDir/data/<name>. The runner does not normally pass that subdir;
// here we honour it so a staged file lands where rtorrent looks.
func (d *RtorrentDriver) SeedPath(savePath, torrentName string) string {
	_ = savePath
	return filepath.Join(d.cfg.DataDir, "data", torrentName)
}

func (d *RtorrentDriver) Start(ctx context.Context, cfg StartConfig) error {
	d.cfg = cfg
	d.container = fmt.Sprintf("bench-rt-%d", time.Now().UnixNano())
	d.scgiAddr = fmt.Sprintf("127.0.0.1:%d", d.HostSCGIPort)

	// jesec/rtorrent's default /etc/rtorrent/rtorrent.rc enables
	// `network.scgi.open_local` (a Unix socket), and rtorrent refuses to
	// re-enable SCGI a second time. We therefore pass `-n` to skip the
	// default rc entirely and supply only the knobs we care about,
	// including TCP SCGI on 0.0.0.0:5000 so the harness can reach it
	// via Docker port mapping.
	// --network host so rtorrent listens directly on the host network.
	// Cross-engine swarm scenarios need this for the same reason rqbit
	// does — the in-process tracker advertises 127.0.0.1 and other
	// engines reach this rtorrent instance via that loopback.
	//
	// We therefore bind the SCGI listener to the per-instance host
	// port (no -p mapping translation), and tell rtorrent's port range
	// to use cfg.ListenPort directly.
	args := []string{
		"run", "-d",
		"--name", d.container,
		"--network", "host",
		"-v", fmt.Sprintf("%s:/rtorrent", cfg.DataDir),
		"--user", "0:0",
		d.Image,
		"-n",
		"-o", "system.daemon.set=true",
		"-o", fmt.Sprintf("network.scgi.open_port=0.0.0.0:%d", d.HostSCGIPort),
		"-o", fmt.Sprintf("network.port_range.set=%d-%d", cfg.ListenPort, cfg.ListenPort),
		"-o", "session.path.set=/rtorrent/sess",
		"-o", "directory.default.set=/rtorrent/data",
	}
	if cfg.DisableDHT {
		args = append(args, "-o", "dht.mode.set=disable")
	}

	// Materialise the data subdirs before docker mounts so the
	// container does not bail with "directory does not exist" when it
	// tries to write the session file.
	for _, sub := range []string{"sess", "data"} {
		if err := exec.Command("mkdir", "-p", fmt.Sprintf("%s/%s", cfg.DataDir, sub)).Run(); err != nil {
			return fmt.Errorf("rtorrent: mkdir: %w", err)
		}
	}

	cmd := exec.CommandContext(ctx, "docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("rtorrent: docker run failed: %v: %s", err, out)
	}
	return d.waitReady(ctx, 30*time.Second)
}

func (d *RtorrentDriver) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := d.callXMLRPC(ctx, "system.client_version", nil)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return fmt.Errorf("rtorrent: SCGI not ready within %s", timeout)
}

// scgiSend dials the SCGI socket, sends a single request, returns the
// raw response body (HTTP-like: status line, headers, blank line, body).
func (d *RtorrentDriver) scgiSend(ctx context.Context, body []byte) ([]byte, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", d.scgiAddr)
	if err != nil {
		return nil, fmt.Errorf("rtorrent: scgi dial: %w", err)
	}
	defer conn.Close()

	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	// SCGI headers are NUL-separated key\0value\0 pairs. CONTENT_LENGTH
	// comes first by spec, then SCGI=1, then anything else. We only
	// need the two mandatory ones.
	var hdr bytes.Buffer
	hdr.WriteString("CONTENT_LENGTH")
	hdr.WriteByte(0)
	hdr.WriteString(strconv.Itoa(len(body)))
	hdr.WriteByte(0)
	hdr.WriteString("SCGI")
	hdr.WriteByte(0)
	hdr.WriteByte('1')
	hdr.WriteByte(0)

	// Wire format: <netstring length>:<headers>,<body>
	var req bytes.Buffer
	req.WriteString(strconv.Itoa(hdr.Len()))
	req.WriteByte(':')
	req.Write(hdr.Bytes())
	req.WriteByte(',')
	req.Write(body)

	if _, err := conn.Write(req.Bytes()); err != nil {
		return nil, fmt.Errorf("rtorrent: scgi write: %w", err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("rtorrent: scgi read: %w", err)
	}
	// Strip the HTTP-style header block (everything up to the blank line).
	if idx := bytes.Index(resp, []byte("\r\n\r\n")); idx >= 0 {
		return resp[idx+4:], nil
	}
	if idx := bytes.Index(resp, []byte("\n\n")); idx >= 0 {
		return resp[idx+2:], nil
	}
	return resp, nil
}

// callXMLRPC marshals a single XML-RPC call, sends it via SCGI, and
// returns the decoded value tree. Only the value types the bench actually
// needs are decoded — int, string, array.
func (d *RtorrentDriver) callXMLRPC(ctx context.Context, method string, params []interface{}) (interface{}, error) {
	body, err := encodeXMLRPC(method, params)
	if err != nil {
		return nil, err
	}
	resp, err := d.scgiSend(ctx, body)
	if err != nil {
		return nil, err
	}
	return decodeXMLRPC(resp)
}

// encodeXMLRPC builds a minimal methodCall document. Supported param
// types: string (handled as base64 if []byte), int64, []interface{} for
// arrays. Unknown types fall through to <string>fmt.Sprint(v)</string>
// which is good enough for rtorrent's typically string-or-int args.
func encodeXMLRPC(method string, params []interface{}) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(`<?xml version="1.0"?><methodCall><methodName>`)
	xml.EscapeText(&b, []byte(method))
	b.WriteString(`</methodName><params>`)
	for _, p := range params {
		b.WriteString(`<param><value>`)
		writeXMLRPCValue(&b, p)
		b.WriteString(`</value></param>`)
	}
	b.WriteString(`</params></methodCall>`)
	return b.Bytes(), nil
}

func writeXMLRPCValue(b *bytes.Buffer, v interface{}) {
	switch val := v.(type) {
	case string:
		b.WriteString(`<string>`)
		xml.EscapeText(b, []byte(val))
		b.WriteString(`</string>`)
	case []byte:
		b.WriteString(`<base64>`)
		b.WriteString(base64.StdEncoding.EncodeToString(val))
		b.WriteString(`</base64>`)
	case int:
		b.WriteString(`<i4>`)
		b.WriteString(strconv.Itoa(val))
		b.WriteString(`</i4>`)
	case int64:
		b.WriteString(`<i8>`)
		b.WriteString(strconv.FormatInt(val, 10))
		b.WriteString(`</i8>`)
	case bool:
		b.WriteString(`<boolean>`)
		if val {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
		b.WriteString(`</boolean>`)
	case []interface{}:
		b.WriteString(`<array><data>`)
		for _, e := range val {
			b.WriteString(`<value>`)
			writeXMLRPCValue(b, e)
			b.WriteString(`</value>`)
		}
		b.WriteString(`</data></array>`)
	default:
		b.WriteString(`<string>`)
		xml.EscapeText(b, []byte(fmt.Sprint(val)))
		b.WriteString(`</string>`)
	}
}

// decodeXMLRPC parses a methodResponse and returns the contained value as
// a Go interface (string, int64, []interface{}, etc.). On fault response
// returns a Go error.
func decodeXMLRPC(data []byte) (interface{}, error) {
	type valueNode struct {
		Inner string `xml:",innerxml"`
	}
	type fault struct {
		Value valueNode `xml:"value"`
	}
	type response struct {
		Params []struct {
			Value valueNode `xml:"value"`
		} `xml:"params>param"`
		Fault *fault `xml:"fault"`
	}
	var r response
	if err := xml.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("rtorrent: xml parse: %w (body: %.200s)", err, data)
	}
	if r.Fault != nil {
		return nil, fmt.Errorf("rtorrent: rpc fault: %s", r.Fault.Value.Inner)
	}
	if len(r.Params) == 0 {
		return nil, nil
	}
	return parseXMLRPCValue(strings.TrimSpace(r.Params[0].Value.Inner))
}

// parseXMLRPCValue turns the inner of a single <value> element into a Go
// scalar or slice. Caller passes the trimmed content (without surrounding
// <value>/</value> tags).
func parseXMLRPCValue(inner string) (interface{}, error) {
	if inner == "" {
		return "", nil
	}
	switch {
	case strings.HasPrefix(inner, "<i4>"):
		n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(inner, "<i4>"), "</i4>"), 10, 64)
		return n, err
	case strings.HasPrefix(inner, "<i8>"):
		n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(inner, "<i8>"), "</i8>"), 10, 64)
		return n, err
	case strings.HasPrefix(inner, "<int>"):
		n, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(inner, "<int>"), "</int>"), 10, 64)
		return n, err
	case strings.HasPrefix(inner, "<string>"):
		return strings.TrimSuffix(strings.TrimPrefix(inner, "<string>"), "</string>"), nil
	case strings.HasPrefix(inner, "<array>"):
		// Crude but sufficient for our usage: extract every <value> child.
		var items []interface{}
		rest := inner
		for {
			start := strings.Index(rest, "<value>")
			if start < 0 {
				break
			}
			end := indexMatchingClose(rest[start:], "value")
			if end < 0 {
				break
			}
			absEnd := start + end
			child := strings.TrimSpace(rest[start+len("<value>") : absEnd])
			val, err := parseXMLRPCValue(child)
			if err != nil {
				return nil, err
			}
			items = append(items, val)
			rest = rest[absEnd+len("</value>"):]
		}
		return items, nil
	}
	// Bare string when the <value> contains plain text (XML-RPC default).
	return inner, nil
}

// indexMatchingClose finds the closing tag matching the first opening
// `<tag>` in s. Returns offset of the matching `</tag>`. Handles nested
// occurrences of the same tag — required for <value><array>...</array></value>
// which itself contains <value> children.
func indexMatchingClose(s, tag string) int {
	open := "<" + tag + ">"
	close := "</" + tag + ">"
	if !strings.HasPrefix(s, open) {
		return -1
	}
	depth := 1
	i := len(open)
	for i < len(s) {
		if strings.HasPrefix(s[i:], open) {
			depth++
			i += len(open)
		} else if strings.HasPrefix(s[i:], close) {
			depth--
			if depth == 0 {
				return i
			}
			i += len(close)
		} else {
			i++
		}
	}
	return -1
}

func (d *RtorrentDriver) AddTorrent(ctx context.Context, t TorrentSpec) error {
	// load.raw_start: take the .torrent bytes inline (base64) and start
	// the torrent immediately. The first arg is an empty target string
	// (rtorrent uses it for "target machine" in distributed setups —
	// always "" for local).
	_, err := d.callXMLRPC(ctx, "load.raw_start", []interface{}{
		"", t.MetaBytes,
	})
	return err
}

func (d *RtorrentDriver) StartTorrent(ctx context.Context, infoHash string) error {
	// `d.start` is the rtorrent-native start by hash. Capitalised form
	// matters for jesec's fork.
	_, err := d.callXMLRPC(ctx, "d.start", []interface{}{strings.ToUpper(infoHash)})
	return err
}

func (d *RtorrentDriver) Stats(ctx context.Context) (Stats, error) {
	// system.multicall avoids paying RTT × 4 for the throttle counters.
	calls := []interface{}{
		[]interface{}{
			map[string]interface{}{"methodName": "throttle.global_up.rate", "params": []interface{}{}},
		},
		// NOTE: rtorrent's system.multicall expects an array of
		// {methodName, params} structs. The Go encoder above does not
		// emit <struct> for maps — for v1 we just issue the calls
		// sequentially. RTT cost is acceptable on localhost.
	}
	_ = calls

	upRate, _ := d.callXMLRPC(ctx, "throttle.global_up.rate", nil)
	dlRate, _ := d.callXMLRPC(ctx, "throttle.global_down.rate", nil)
	upTotal, _ := d.callXMLRPC(ctx, "throttle.global_up.total", nil)
	dlTotal, _ := d.callXMLRPC(ctx, "throttle.global_down.total", nil)

	dlList, _ := d.callXMLRPC(ctx, "download_list", nil)
	var torrentCount int
	if arr, ok := dlList.([]interface{}); ok {
		torrentCount = len(arr)
	}

	return Stats{
		UploadRate:      asUint64(upRate),
		DownloadRate:    asUint64(dlRate),
		UploadedTotal:   asUint64(upTotal),
		DownloadedTotal: asUint64(dlTotal),
		TorrentsTotal:   torrentCount,
		// Active torrents and per-engine peer count would require a
		// per-torrent multicall (`d.peers_connected`); we leave them
		// at zero for v1 to keep the sample loop cheap.
		TorrentsActive: 0,
		PeersConnected: 0,
	}, nil
}

func asUint64(v interface{}) uint64 {
	if n, ok := v.(int64); ok && n > 0 {
		return uint64(n)
	}
	return 0
}

func (d *RtorrentDriver) Stop(ctx context.Context) error {
	if d.container == "" {
		return nil
	}
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", d.container).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		return fmt.Errorf("rtorrent: docker rm: %v: %s", err, out)
	}
	return nil
}
