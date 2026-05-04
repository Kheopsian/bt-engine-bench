// Package engine defines the contract every BitTorrent engine driver
// must satisfy so that the harness can treat them interchangeably.
//
// Drivers are responsible for:
//   - bringing up the engine (typically as a Docker container or a local
//     subprocess) with a deterministic config;
//   - translating the harness's torrent operations into engine-native API
//     calls;
//   - exposing engine-side stats in a normalised shape;
//   - tearing the engine down cleanly.
//
// Drivers MUST NOT leak engine-specific concepts into the public surface.
// If an engine has no equivalent for a given operation, return ErrUnsupported.
package engine

import (
	"context"
	"errors"
)

// ErrUnsupported signals that the driver's underlying engine has no
// equivalent for the requested operation. Callers can decide to skip the
// scenario step instead of failing the whole run.
var ErrUnsupported = errors.New("operation not supported by this engine")

// Driver is the interface every engine implementation provides.
type Driver interface {
	// Name is a stable short identifier (e.g. "typhon", "rqbit",
	// "libtorrent", "transmission"). Used as a label in CSV output and
	// as a primary key when the harness compares runs side by side.
	Name() string

	// Start brings the engine up with the given config and blocks until
	// it is ready to accept torrent operations (or the context expires).
	Start(ctx context.Context, cfg StartConfig) error

	// AddTorrent registers a torrent with the engine. The raw .torrent
	// bytes are passed; magnets are out of scope for v1.
	AddTorrent(ctx context.Context, t TorrentSpec) error

	// StartTorrent transitions a previously-added torrent to the active
	// state. Some engines auto-start on add; in that case this is a no-op.
	StartTorrent(ctx context.Context, infoHash string) error

	// Stats returns engine-wide aggregate statistics. Sampled at the
	// harness's metric tick rate.
	Stats(ctx context.Context) (Stats, error)

	// Stop tears the engine down, releasing all resources. Must be safe
	// to call even if Start failed.
	Stop(ctx context.Context) error
}

// StartConfig captures the knobs every engine respects in some form. Each
// driver maps these to the engine's native config language.
type StartConfig struct {
	// DataDir is the writable directory the engine should use for both
	// torrent payload and any internal state (resume data, db files).
	DataDir string

	// ListenPort is the BitTorrent listen port (TCP + uTP).
	ListenPort int

	// MaxPeersPerTorrent caps the per-torrent peer connection list.
	// Zero means leave the engine default.
	MaxPeersPerTorrent int

	// MaxTotalConnections caps the global connection list across all
	// torrents. Zero means leave the engine default.
	MaxTotalConnections int

	// DisableDHT, DisablePEX, DisableLSD turn off peer-discovery
	// mechanisms when a scenario wants to isolate one source. Defaults
	// (false) keep all of them on.
	DisableDHT bool
	DisablePEX bool
	DisableLSD bool
}

// TorrentSpec describes a single torrent the harness wants the engine to
// handle.
type TorrentSpec struct {
	// InfoHash is the hex-encoded info_hash. Drivers use it as the key
	// in subsequent operations.
	InfoHash string

	// MetaBytes is the raw .torrent file contents.
	MetaBytes []byte

	// SavePath is the directory the engine should write/read payload to.
	// Must be inside StartConfig.DataDir.
	SavePath string
}

// Stats is the normalised aggregate snapshot every driver returns. Counters
// are cumulative since engine start, rates are instantaneous (engine's own
// 1s smoothing is fine — the harness does not re-derive them).
type Stats struct {
	// UploadRate / DownloadRate in bytes per second.
	UploadRate   uint64
	DownloadRate uint64

	// UploadedTotal / DownloadedTotal in bytes since engine start.
	UploadedTotal   uint64
	DownloadedTotal uint64

	// TorrentsTotal is the total registered torrents (started + paused).
	TorrentsTotal int

	// TorrentsActive is torrents currently exchanging data with at least
	// one peer.
	TorrentsActive int

	// PeersConnected is the total connected peer count across all
	// torrents.
	PeersConnected int
}
