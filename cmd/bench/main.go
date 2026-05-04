// Command bench is the entry point for the cross-engine BitTorrent
// benchmark harness.
//
//	bench compare \
//	    --engines typhon,rqbit \
//	    --scenario scenarios/smoke.json \
//	    --output run.csv \
//	    --typhon-bin /usr/local/bin/hydra-engine
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Kheopsian/bt-engine-bench/internal/engine"
	"github.com/Kheopsian/bt-engine-bench/internal/metrics"
	"github.com/Kheopsian/bt-engine-bench/internal/runner"
	"github.com/Kheopsian/bt-engine-bench/internal/scenario"
	"github.com/Kheopsian/bt-engine-bench/internal/torrentgen"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "compare":
		runCompare(os.Args[2:])
	case "gentorrent":
		runGentorrent(os.Args[2:])
	case "version":
		fmt.Println("bt-engine-bench dev")
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `bt-engine-bench — cross-engine BitTorrent benchmark harness

Usage:
  bench compare [flags]      Run the same scenario across multiple engines.
  bench gentorrent [flags]   Build a .torrent file from a payload.
  bench version              Print version.
  bench help                 Show this help.

Run 'bench compare -h' or 'bench gentorrent -h' for flag lists.`)
}

// runGentorrent assembles a .torrent file. Two input modes:
//   - --payload PATH  : wrap an existing file
//   - --random N      : generate a random payload of N bytes first, then wrap it
//
// Random payloads are deterministic-looking from outside (hex-named files
// with predictable size) but their content is fresh each run — fine for a
// bench, since two engines downloading the same `--random` torrent always
// see the same hashes within one harness invocation.
func runGentorrent(args []string) {
	fs := flag.NewFlagSet("gentorrent", flag.ExitOnError)
	payload := fs.String("payload", "", "path to existing payload file")
	randomBytes := fs.Int64("random", 0, "generate a random payload of N bytes (alternative to --payload)")
	out := fs.String("out", "", "output .torrent path (required)")
	payloadOut := fs.String("payload-out", "", "where to write the random payload (only with --random; defaults to <out>.payload)")
	pieceLen := fs.Int64("piece-length", 256*1024, "piece length in bytes")
	announce := fs.String("announce", "", "tracker URL (optional)")
	name := fs.String("name", "", "torrent name (defaults to payload basename)")
	_ = fs.Parse(args)

	if *out == "" || (*payload == "" && *randomBytes == 0) {
		fs.Usage()
		os.Exit(2)
	}

	srcPath := *payload
	if *randomBytes > 0 {
		dest := *payloadOut
		if dest == "" {
			dest = *out + ".payload"
		}
		if err := writeRandomPayload(dest, *randomBytes); err != nil {
			log.Fatalf("gentorrent: %v", err)
		}
		srcPath = dest
		log.Printf("gentorrent: wrote %d random bytes to %s", *randomBytes, dest)
	}

	res, err := torrentgen.GenerateFile(torrentgen.Spec{
		PayloadPath: srcPath,
		PieceLength: *pieceLen,
		AnnounceURL: *announce,
		Name:        *name,
	})
	if err != nil {
		log.Fatalf("gentorrent: %v", err)
	}
	if err := os.WriteFile(*out, res.Torrent, 0o644); err != nil {
		log.Fatalf("gentorrent: write %s: %v", *out, err)
	}
	log.Printf("gentorrent: wrote %s — info_hash=%s size=%d bytes",
		*out, hex.EncodeToString(res.InfoHash[:]), res.Size)
}

func writeRandomPayload(path string, size int64) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create payload: %w", err)
	}
	defer f.Close()
	if _, err := io.CopyN(f, rand.Reader, size); err != nil {
		return fmt.Errorf("write random bytes: %w", err)
	}
	return nil
}

func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	enginesArg := fs.String("engines", "", "comma-separated engine names (typhon, rqbit, transmission, libtorrent, rain, rtorrent)")
	scenarioPath := fs.String("scenario", "", "path to scenario JSON")
	output := fs.String("output", "run.csv", "output CSV path")
	workDir := fs.String("work-dir", "", "working dir for engine state (default: temp)")
	typhonBin := fs.String("typhon-bin", "/usr/local/bin/hydra-engine", "path to typhon binary (only required if 'typhon' is in --engines)")
	rqbitImage := fs.String("rqbit-image", "ikatson/rqbit:latest", "Docker image for rqbit (only required if 'rqbit' is in --engines)")
	rqbitAPIPort := fs.Int("rqbit-api-port", 13030, "host-side port for rqbit HTTP API")
	rqbitListenPort := fs.Int("rqbit-listen-port", 14240, "host-side port for rqbit BitTorrent listen")
	trImage := fs.String("transmission-image", "linuxserver/transmission:latest", "Docker image for transmission")
	trAPIPort := fs.Int("transmission-api-port", 19091, "host-side port for transmission RPC")
	trListenPort := fs.Int("transmission-listen-port", 19092, "host-side port for transmission BitTorrent listen")
	qbImage := fs.String("qbit-image", "linuxserver/qbittorrent:latest", "Docker image for qbittorrent (libtorrent stand-in)")
	qbAPIPort := fs.Int("qbit-api-port", 18080, "host-side port for qbit WebUI")
	qbListenPort := fs.Int("qbit-listen-port", 18881, "host-side port for qbit BitTorrent listen")
	rainBin := fs.String("rain-bin", "/usr/local/bin/rain", "path to cenkalti/rain binary (only required if 'rain' is in --engines)")
	rainRPCPort := fs.Int("rain-rpc-port", 17246, "host-side port for rain JSON-RPC")
	rtImage := fs.String("rtorrent-image", "jesec/rtorrent:latest", "Docker image for rtorrent")
	rtSCGIPort := fs.Int("rtorrent-scgi-port", 15000, "host-side port for rtorrent SCGI")
	rtListenPort := fs.Int("rtorrent-listen-port", 15010, "host-side port for rtorrent BitTorrent listen")
	_ = fs.Parse(args)

	if *enginesArg == "" || *scenarioPath == "" {
		fs.Usage()
		os.Exit(2)
	}

	sc, err := scenario.Load(*scenarioPath)
	if err != nil {
		log.Fatal(err)
	}

	wd := *workDir
	if wd == "" {
		wd, err = os.MkdirTemp("", "bench-")
		if err != nil {
			log.Fatalf("bench: mktempdir: %v", err)
		}
		log.Printf("bench: working dir = %s", wd)
	}

	wd, err = filepath.Abs(wd)
	if err != nil {
		log.Fatal(err)
	}

	drivers, err := buildDrivers(*enginesArg,
		*typhonBin,
		*rqbitImage, *rqbitAPIPort, *rqbitListenPort,
		*trImage, *trAPIPort, *trListenPort,
		*qbImage, *qbAPIPort, *qbListenPort,
		*rainBin, *rainRPCPort,
		*rtImage, *rtSCGIPort, *rtListenPort,
	)
	if err != nil {
		log.Fatal(err)
	}

	out, err := metrics.New(*output, sc.Name)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Printf("bench: close output: %v", err)
		}
	}()

	// SIGINT/SIGTERM => cancel context, runner stops + engines tear down.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	start := time.Now()
	log.Printf("bench: scenario=%q duration=%s engines=%s", sc.Name, sc.Duration.Std(), *enginesArg)
	if err := runner.Run(ctx, sc, drivers, wd, out); err != nil {
		log.Fatalf("bench: %v", err)
	}
	log.Printf("bench: done in %s. samples written to %s", time.Since(start).Round(100*time.Millisecond), *output)
}

// buildDrivers translates the --engines flag into concrete Driver values,
// returning a stable error message if the user asked for an unknown name.
func buildDrivers(
	list, typhonBin string,
	rqbitImage string, rqbitAPIPort, rqbitListenPort int,
	trImage string, trAPIPort, trListenPort int,
	qbImage string, qbAPIPort, qbListenPort int,
	rainBin string, rainRPCPort int,
	rtImage string, rtSCGIPort, rtListenPort int,
) ([]engine.Driver, error) {
	var out []engine.Driver
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		switch name {
		case "typhon":
			out = append(out, engine.NewTyphonDriver(typhonBin))
		case "rqbit":
			d := engine.NewRqbitDriver()
			d.Image = rqbitImage
			d.HostAPIPort = rqbitAPIPort
			d.HostListenPort = rqbitListenPort
			out = append(out, d)
		case "transmission":
			d := engine.NewTransmissionDriver()
			d.Image = trImage
			d.HostPort = trAPIPort
			d.HostListenPort = trListenPort
			out = append(out, d)
		case "libtorrent":
			d := engine.NewQbitDriver()
			d.Image = qbImage
			d.HostPort = qbAPIPort
			d.HostListenPort = qbListenPort
			out = append(out, d)
		case "rain":
			d := engine.NewRainDriver(rainBin)
			d.HostRPCPort = rainRPCPort
			out = append(out, d)
		case "rtorrent":
			d := engine.NewRtorrentDriver()
			d.Image = rtImage
			d.HostSCGIPort = rtSCGIPort
			d.HostListenPort = rtListenPort
			out = append(out, d)
		case "":
			continue
		default:
			return nil, fmt.Errorf("unknown engine %q (supported: typhon, rqbit, transmission, libtorrent, rain, rtorrent)", name)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no engines selected")
	}
	return out, nil
}
