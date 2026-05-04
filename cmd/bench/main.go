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
	"flag"
	"fmt"
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
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "compare":
		runCompare(os.Args[2:])
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
  bench compare [flags]    Run the same scenario across multiple engines.
  bench version            Print version.
  bench help               Show this help.

Run 'bench compare -h' for the flag list.`)
}

func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	enginesArg := fs.String("engines", "", "comma-separated engine names (typhon, rqbit, transmission, libtorrent)")
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
		case "":
			continue
		default:
			return nil, fmt.Errorf("unknown engine %q (supported: typhon, rqbit, transmission, libtorrent)", name)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no engines selected")
	}
	return out, nil
}
