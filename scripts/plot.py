#!/usr/bin/env python3
"""
Plot a bench CSV: time-series of throughput, peer count and torrent counts
per engine, plus a final-state summary table.

Usage:
    python3 scripts/plot.py run.csv out.png

The script depends only on stdlib + pandas + matplotlib so any run-of-the-mill
data-science setup works. No seaborn, no plotly — keep the deps trivial.

Schema assumed (positional, see internal/metrics/csv.go):
    ts_unix_ms, scenario, engine, upload_rate_bps, download_rate_bps,
    uploaded_total_bytes, downloaded_total_bytes,
    torrents_total, torrents_active, peers_connected
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def fmt_bytes(n: float) -> str:
    """Human-readable bytes/sec (1024-base, MiB-style)."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def load(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["ts"] = pd.to_datetime(df["ts_unix_ms"], unit="ms")
    df["t_seconds"] = (df["ts"] - df["ts"].min()).dt.total_seconds()
    return df


def plot(df: pd.DataFrame, out_path: Path) -> None:
    engines = sorted(df["engine"].unique())
    scenario = df["scenario"].iloc[0]

    # 4 stacked subplots — UL rate, DL rate, peers, torrents
    fig, axes = plt.subplots(4, 1, figsize=(11, 12), sharex=True)
    fig.suptitle(f"bt-engine-bench — scenario: {scenario}", fontsize=14, y=0.995)

    panels = [
        ("upload_rate_bps", axes[0], "Upload rate (B/s)", "log"),
        ("download_rate_bps", axes[1], "Download rate (B/s)", "log"),
        ("peers_connected", axes[2], "Connected peers", "linear"),
        ("torrents_total", axes[3], "Torrents (total)", "linear"),
    ]
    for col, ax, ylabel, scale in panels:
        for eng in engines:
            sub = df[df["engine"] == eng].sort_values("t_seconds")
            ax.plot(sub["t_seconds"], sub[col], label=eng, linewidth=1.5)
        ax.set_ylabel(ylabel)
        # Log scale only when there is at least one positive sample —
        # empty/all-zero series on log axes are an axis warning + an
        # ugly empty plot, so degrade gracefully.
        if scale == "log" and (df[col] > 0).any():
            ax.set_yscale("log")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right", fontsize=9)

    axes[-1].set_xlabel("Time (seconds since first sample)")
    plt.tight_layout(rect=(0, 0, 1, 0.99))
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    print(f"wrote {out_path}", file=sys.stderr)


def summary(df: pd.DataFrame) -> None:
    """Print a per-engine final-state table to stdout. Useful in CI logs."""
    last = df.sort_values("ts_unix_ms").groupby("engine").tail(1)
    print()
    print(f"{'engine':<14} {'final UL/s':>12} {'final DL/s':>12} {'UL total':>12} {'DL total':>12} {'peers':>6} {'torr.':>6}")
    print("-" * 80)
    for _, r in last.sort_values("engine").iterrows():
        print(
            f"{r['engine']:<14} "
            f"{fmt_bytes(r['upload_rate_bps']):>12} "
            f"{fmt_bytes(r['download_rate_bps']):>12} "
            f"{fmt_bytes(r['uploaded_total_bytes']):>12} "
            f"{fmt_bytes(r['downloaded_total_bytes']):>12} "
            f"{int(r['peers_connected']):>6} "
            f"{int(r['torrents_total']):>6}"
        )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("csv", type=Path, help="input bench CSV")
    p.add_argument("out", type=Path, help="output PNG path")
    args = p.parse_args()

    df = load(args.csv)
    if df.empty:
        print("input CSV has no rows", file=sys.stderr)
        return 1
    plot(df, args.out)
    summary(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
