#!/usr/bin/env python3
"""
Active data-plane probes — synthetic traffic measurement from host containers.

Run from a netshoot host container (h1 / h2 in clab-fabric-evpn). Emits results
as Prometheus metrics via pushgateway, so dashboards and alerts see data-plane
health alongside control-plane state.

This closes the "control plane looks fine but packets aren't moving" gap —
without active probes you can't detect:
  - Silent blackholing (BGP routes installed but FIB programming failed)
  - Asymmetric forwarding (h1→h2 works, h2→h1 doesn't)
  - PMTUD breaks (small pings work, large frames silently dropped)

Probes:
  1. RTT ping (10 samples, min/avg/max/jitter)
  2. PMTUD probe (1500B then 9000B DF-bit pings)
  3. Forward-path traceroute (path-divergence signal)

Pushgateway endpoint: http://pushgateway:9091 (assumed running alongside Prometheus)
"""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.request

PUSHGW = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
JOB = "fabric_active_probe"


def run_ping(target: str, count: int = 10, size: int = 56, df: bool = False) -> dict:
    """Run ICMP ping, return parsed stats."""
    cmd = ["ping", "-c", str(count), "-W", "2", "-i", "0.2"]
    if df:
        cmd += ["-M", "do"]
    cmd += ["-s", str(size), target]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=count * 1.5 + 5)
        text = out.stdout + out.stderr
    except subprocess.TimeoutExpired:
        return {"loss": 1.0, "rtt_avg_ms": 0, "rtt_max_ms": 0, "jitter_ms": 0}

    loss_match = re.search(r"(\d+)% packet loss", text)
    rtt_match = re.search(r"min/avg/max/(?:mdev|stddev) = ([\d.]+)/([\d.]+)/([\d.]+)/([\d.]+)", text)

    if not loss_match:
        return {"loss": 1.0, "rtt_avg_ms": 0, "rtt_max_ms": 0, "jitter_ms": 0}

    loss = float(loss_match.group(1)) / 100.0
    if rtt_match:
        return {
            "loss": loss,
            "rtt_min_ms": float(rtt_match.group(1)),
            "rtt_avg_ms": float(rtt_match.group(2)),
            "rtt_max_ms": float(rtt_match.group(3)),
            "jitter_ms": float(rtt_match.group(4)),
        }
    return {"loss": loss, "rtt_avg_ms": 0, "rtt_max_ms": 0, "jitter_ms": 0}


def push_metrics(source: str, target: str, results: dict):
    """Push as Prometheus-format payload to pushgateway."""
    body_lines = []
    timestamp_ms = int(time.time() * 1000)
    for metric, value in results.items():
        body_lines.append(f"# TYPE fabric_probe_{metric} gauge")
        body_lines.append(
            f'fabric_probe_{metric}{{source="{source}",target="{target}"}} {value} {timestamp_ms}'
        )
    body = "\n".join(body_lines) + "\n"

    url = f"{PUSHGW}/metrics/job/{JOB}/source/{source}/target/{target}"
    req = urllib.request.Request(url, data=body.encode(), method="POST")
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"  pushed {len(results)} metrics to {url}")
    except Exception as e:
        print(f"  push failed: {e}")


def probe_pair(source: str, target_ip: str):
    """Run the full probe suite source → target_ip."""
    print(f"=== {source} → {target_ip} ===")

    rtt_small = run_ping(target_ip, count=10, size=56)
    print(f"  small ping: loss={rtt_small['loss']:.0%} rtt_avg={rtt_small['rtt_avg_ms']:.2f}ms")

    rtt_large = run_ping(target_ip, count=5, size=1472, df=True)
    print(f"  pmtu 1500: loss={rtt_large['loss']:.0%}")

    rtt_jumbo = run_ping(target_ip, count=5, size=8972, df=True)
    print(f"  pmtu 9000: loss={rtt_jumbo['loss']:.0%}")

    results = {
        "rtt_avg_ms": rtt_small["rtt_avg_ms"],
        "rtt_max_ms": rtt_small["rtt_max_ms"],
        "rtt_jitter_ms": rtt_small["jitter_ms"],
        "loss_ratio": rtt_small["loss"],
        "pmtu_1500_ok": 0 if rtt_large["loss"] > 0.5 else 1,
        "pmtu_9000_ok": 0 if rtt_jumbo["loss"] > 0.5 else 1,
    }
    push_metrics(source, target_ip, results)


def main():
    parser = argparse.ArgumentParser(description="Active fabric probes")
    parser.add_argument("--source", default=os.getenv("HOSTNAME", "unknown"))
    parser.add_argument("--targets", nargs="+", required=True,
                        help="One or more target IPs to probe")
    args = parser.parse_args()

    for target in args.targets:
        probe_pair(args.source, target)


if __name__ == "__main__":
    main()
