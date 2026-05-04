// Command bench is the entry point for the cross-engine BitTorrent
// benchmark harness. v1 supports a single subcommand:
//
//	bench compare --engines <list> --scenario <path> --duration <go-duration> --output <csv>
//
// Subsequent versions will add scenario authoring helpers, replay tooling,
// and plot generation.
package main

import (
	"flag"
	"fmt"
	"os"
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
  bench help               Show this help.`)
}

func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	engines := fs.String("engines", "", "comma-separated engine names (typhon, rqbit, transmission, libtorrent)")
	scenario := fs.String("scenario", "", "path to scenario YAML")
	duration := fs.String("duration", "300s", "scenario duration (Go duration syntax)")
	output := fs.String("output", "run.csv", "output CSV path")
	_ = fs.Parse(args)

	if *engines == "" || *scenario == "" {
		fs.Usage()
		os.Exit(2)
	}

	// TODO: load scenario, instantiate drivers, hand off to runner.
	fmt.Printf("compare: engines=%s scenario=%s duration=%s output=%s\n",
		*engines, *scenario, *duration, *output)
	fmt.Fprintln(os.Stderr, "not implemented yet")
	os.Exit(1)
}
