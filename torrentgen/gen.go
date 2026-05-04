// Package torrentgen produces synthetic .torrent files from a payload on
// disk. v1 supports single-file torrents only — directory layouts are not
// in scope yet.
//
// The harness uses this to construct deterministic workloads: callers pre-
// generate a payload (random bytes, a real file, etc.), feed it here, then
// hand the resulting .torrent to every engine in the run.
package torrentgen

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Kheopsian/bt-engine-bench/internal/bencode"
)

// Spec captures the input to GenerateFile.
type Spec struct {
	// PayloadPath is the on-disk file to wrap in a torrent. Must exist.
	PayloadPath string

	// PieceLength is the piece size in bytes. Common values: 16 KiB,
	// 256 KiB, 1 MiB, 4 MiB. Power of two recommended but not enforced.
	// Zero defaults to 256 KiB.
	PieceLength int64

	// AnnounceURL is the tracker URL to embed. Empty means "trackerless"
	// (DHT/PEX only). For the bench's internal tracker scenarios this
	// is something like http://127.0.0.1:6969/announce.
	AnnounceURL string

	// Name is the torrent's `info.name` field. Defaults to the payload
	// file's basename.
	Name string

	// CreationDate is the optional `creation date` field. If zero, set
	// to time.Now().
	CreationDate time.Time
}

// Result bundles the .torrent bytes with derived metadata callers usually
// want — the info_hash (for engine RPC calls) and the total size.
type Result struct {
	Torrent  []byte
	InfoHash [20]byte
	Size     int64
}

// GenerateFile builds a single-file .torrent from spec. The piece hashes
// are computed in a streaming fashion so the call works on payloads larger
// than RAM.
func GenerateFile(spec Spec) (*Result, error) {
	if spec.PieceLength <= 0 {
		spec.PieceLength = 256 * 1024
	}
	if spec.PayloadPath == "" {
		return nil, fmt.Errorf("torrentgen: PayloadPath is required")
	}

	f, err := os.Open(spec.PayloadPath)
	if err != nil {
		return nil, fmt.Errorf("torrentgen: open payload: %w", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("torrentgen: stat payload: %w", err)
	}
	size := st.Size()
	if size == 0 {
		return nil, fmt.Errorf("torrentgen: empty payload not allowed")
	}

	// Compute concatenated SHA-1 of every piece. Final piece may be
	// shorter than PieceLength; that is correct per BEP-3.
	pieces, err := hashPieces(f, spec.PieceLength)
	if err != nil {
		return nil, err
	}

	name := spec.Name
	if name == "" {
		name = filepath.Base(spec.PayloadPath)
	}

	info := bencode.Dict{
		"name":         name,
		"length":       size,
		"piece length": spec.PieceLength,
		"pieces":       pieces,
	}
	infoBytes, err := bencode.Encode(info)
	if err != nil {
		return nil, fmt.Errorf("torrentgen: encode info: %w", err)
	}
	infoHash := sha1.Sum(infoBytes)

	creation := spec.CreationDate
	if creation.IsZero() {
		creation = time.Now()
	}

	root := bencode.Dict{
		"info":          info,
		"creation date": creation.Unix(),
		"created by":    "bt-engine-bench/torrentgen",
	}
	if spec.AnnounceURL != "" {
		root["announce"] = spec.AnnounceURL
	}
	torrent, err := bencode.Encode(root)
	if err != nil {
		return nil, fmt.Errorf("torrentgen: encode root: %w", err)
	}

	return &Result{
		Torrent:  torrent,
		InfoHash: infoHash,
		Size:     size,
	}, nil
}

func hashPieces(r io.Reader, pieceLen int64) ([]byte, error) {
	if pieceLen <= 0 {
		return nil, fmt.Errorf("torrentgen: piece length must be positive")
	}
	buf := make([]byte, pieceLen)
	var pieces []byte
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			h := sha1.Sum(buf[:n])
			pieces = append(pieces, h[:]...)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("torrentgen: read piece: %w", err)
		}
	}
	return pieces, nil
}
