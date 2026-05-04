// Package tracker implements a minimal BEP-3 HTTP BitTorrent tracker so the
// bench harness can drive cross-engine swarms without depending on external
// infrastructure. The tracker is in-process: callers Start() it on an
// address, scenarios embed that announce URL into their .torrent files,
// engines GET /announce, and the tracker tells them about every other
// engine in the swarm.
//
// Scope:
//   - HTTP only (no UDP), no scrape, no auth, no compact-only enforcement
//     (we always return both compact and dict forms so picky engines work).
//   - Per-info-hash peer set with 30 minute TTL — enough for a bench run.
//   - In-memory only; restart loses all swarm state, which is what we want.
package tracker

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Kheopsian/bt-engine-bench/internal/bencode"
)

// Server is a single tracker instance.
type Server struct {
	httpSrv *http.Server

	mu     sync.Mutex
	swarms map[[20]byte]map[string]*peerEntry // info_hash → peer_id → entry
}

type peerEntry struct {
	ip   net.IP
	port uint16
	last time.Time
}

const (
	announceInterval = 30 * time.Second
	peerTTL          = 30 * time.Minute
)

// Start brings the tracker up listening on listenAddr (e.g. "0.0.0.0:6969")
// and returns once the listener is bound. The actual address (after
// resolving :0 to a real port) is exposed via Addr().
func Start(listenAddr string) (*Server, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("tracker: listen %s: %w", listenAddr, err)
	}
	s := &Server{
		swarms: make(map[[20]byte]map[string]*peerEntry),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/announce", s.handleAnnounce)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Anything other than /announce gets a polite 404. We do not
		// implement /scrape; engines that hit it cope with the empty
		// response.
		http.NotFound(w, r)
	})
	s.httpSrv = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() { _ = s.httpSrv.Serve(ln) }()
	go s.gcLoop()
	return s, nil
}

// Addr returns the host:port the tracker is listening on. Useful when the
// caller passed ":0" to Start.
func (s *Server) Addr() string { return s.httpSrv.Addr }

// Stop shuts the tracker down. Safe to call multiple times.
func (s *Server) Stop() {
	if s.httpSrv != nil {
		_ = s.httpSrv.Close()
	}
}

// gcLoop drops stale peer entries every minute. Without it long-running
// runs accumulate ghost peers and engines waste connect attempts on dead
// IPs.
func (s *Server) gcLoop() {
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		cutoff := time.Now().Add(-peerTTL)
		for ih, swarm := range s.swarms {
			for id, p := range swarm {
				if p.last.Before(cutoff) {
					delete(swarm, id)
				}
			}
			if len(swarm) == 0 {
				delete(s.swarms, ih)
			}
		}
		s.mu.Unlock()
	}
}

func (s *Server) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// info_hash and peer_id are 20 raw bytes URL-encoded as %XX. Go's
	// url.Values already decodes them, but bytes outside ASCII land as
	// raw bytes inside the string — accept any 20-byte sequence.
	infoHashStr := q.Get("info_hash")
	peerIDStr := q.Get("peer_id")
	if len(infoHashStr) != 20 || len(peerIDStr) != 20 {
		http.Error(w, "missing or malformed info_hash/peer_id", http.StatusBadRequest)
		return
	}
	var ih [20]byte
	copy(ih[:], infoHashStr)

	portStr := q.Get("port")
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		http.Error(w, "bad port", http.StatusBadRequest)
		return
	}

	// Source IP: prefer the announce-supplied `ip` parameter (lets the
	// harness force loopback connectivity even when the engine binds
	// elsewhere), fall back to the connection's remote address.
	ipStr := q.Get("ip")
	var ip net.IP
	if ipStr != "" {
		ip = net.ParseIP(ipStr)
	}
	if ip == nil {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		ip = net.ParseIP(host)
	}
	if ip == nil {
		http.Error(w, "could not determine peer ip", http.StatusBadRequest)
		return
	}

	event := q.Get("event")

	s.mu.Lock()
	swarm, ok := s.swarms[ih]
	if !ok {
		swarm = make(map[string]*peerEntry)
		s.swarms[ih] = swarm
	}
	if event == "stopped" {
		delete(swarm, peerIDStr)
	} else {
		swarm[peerIDStr] = &peerEntry{
			ip:   ip,
			port: uint16(port),
			last: time.Now(),
		}
	}

	// Build peer list excluding the requesting peer (no point giving the
	// caller its own address back).
	v4Peers := make([]byte, 0, 6*len(swarm))
	dictPeers := make([]bencode.Value, 0, len(swarm))
	for id, p := range swarm {
		if id == peerIDStr {
			continue
		}
		if ip4 := p.ip.To4(); ip4 != nil {
			v4Peers = append(v4Peers, ip4...)
			v4Peers = append(v4Peers, byte(p.port>>8), byte(p.port&0xff))
		}
		dictPeers = append(dictPeers, bencode.Dict{
			"peer id": id,
			"ip":      p.ip.String(),
			"port":    int64(p.port),
		})
	}
	s.mu.Unlock()

	resp := bencode.Dict{
		"interval":     int64(announceInterval / time.Second),
		"min interval": int64(announceInterval / time.Second),
		// Always emit BOTH formats. Compact-supporting engines pick
		// `peers` (string), older clients pick the dict list. Spec
		// allows both in one response.
		"peers":  v4Peers,
		"peers6": []byte{}, // empty IPv6 list — bench is v4-only for now
		"complete":   int64(0),
		"incomplete": int64(len(dictPeers)),
	}
	body, err := bencode.Encode(resp)
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	// Some engines insist on receiving the dict-form when they sent
	// `compact=0`. Honour that explicitly.
	if q.Get("compact") == "0" {
		resp["peers"] = []bencode.Value(dictPeers)
		body, _ = bencode.Encode(resp)
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(body)
}
