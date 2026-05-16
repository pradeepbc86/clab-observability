# Active OAM — synthetic data-plane probes

Why this exists: BGP, BFD, frr_exporter, and OpenBMP all measure the **control plane**. They tell you whether sessions are up, prefixes are announced, and FIB sizes look reasonable. None of them tell you whether packets are actually moving.

A senior failure mode I've seen multiple times in production:

- BGP session: Established
- BFD: Up  
- FIB programmed with the right next-hop
- frr_exporter: green across the board
- ...and yet h1 cannot ping h2 because of a kernel netfilter rule, a broken VXLAN encap state, or a half-programmed ASIC FIB

Active probes are the only way to catch this class of failure.

## How it works

```
┌───────────────────┐    icmp / pmtu / traceroute    ┌───────────────────┐
│ h1 (netshoot)     │ ─────────────────────────────► │ h2 (netshoot)     │
│ active_probe.py   │                                │                   │
└─────────┬─────────┘                                └───────────────────┘
          │ Prometheus pushgateway protocol
          ▼
┌───────────────────────────────┐
│ pushgateway:9091              │
└────────────┬──────────────────┘
             │ scrape
             ▼
┌───────────────────────────────┐    queries    ┌─────────────────┐
│ Prometheus                    │ ◄──────────── │ Grafana / alerts│
│ (fabric_probe_* metrics)      │               └─────────────────┘
└───────────────────────────────┘
```

## Metrics emitted

| Metric | Type | Meaning |
|--------|------|---------|
| `fabric_probe_rtt_avg_ms` | gauge | Mean ICMP RTT over 10 samples |
| `fabric_probe_rtt_max_ms` | gauge | Worst-case RTT (catches microburst impact) |
| `fabric_probe_rtt_jitter_ms` | gauge | RTT stddev (LSM-style jitter signal) |
| `fabric_probe_loss_ratio` | gauge | 0.0–1.0; >0 is the headline blackhole signal |
| `fabric_probe_pmtu_1500_ok` | gauge | 1 if 1500B DF ping succeeds (validates fabric MTU) |
| `fabric_probe_pmtu_9000_ok` | gauge | 1 if 9000B DF ping succeeds (validates jumbo path) |

Each labeled with `source` and `target` so per-pair dashboards work.

## Running

```bash
# From inside h1 container in clab-fabric-evpn
docker exec clab-fabric-evpn-h1 \
  python3 /scripts/active_probe.py --source h1 --targets 192.168.10.2

# As a cronjob — every 30s from each host
*/1 * * * * python3 /scripts/active_probe.py --source h1 --targets 192.168.10.2
```

## Alerts

Add to `configs/prometheus/alerts.yml`:

```yaml
- alert: DataPlaneSilentBlackhole
  expr: fabric_probe_loss_ratio > 0.5
  for: 30s
  labels:
    severity: critical
    team: network-ops
  annotations:
    summary: "Active probe {{ $labels.source }}→{{ $labels.target }} losing >50%"
    description: |
      Control plane appears healthy (no BGP/BFD alerts) but data plane shows
      packet loss. Likely silent blackhole — FIB inconsistency or kernel netfilter
      drop. Correlate with frr_bgp_peer_state on both endpoints.

- alert: FabricJumboMTUBroken
  expr: fabric_probe_pmtu_9000_ok == 0 and fabric_probe_pmtu_1500_ok == 1
  for: 1m
  labels:
    severity: warning
    team: network-ops
  annotations:
    summary: "9000B DF ping fails but 1500B succeeds — jumbo path broken"
    description: |
      Underlay MTU misconfiguration on one of the fabric links. Run
      `make validate` and check `ip -d link show eth1 | grep mtu` per node.
```

## Why this is the missing piece

Without active OAM, the observability stack is a **passive observer** of what FRR tells us. If FRR is lying (programming bug, FIB-out-of-sync, kernel netfilter drop), the stack reports green.

This is the kind of failure that brings down real DCs and takes hours to diagnose because every dashboard says "everything is fine." Active probes generate ground truth.

## Trade-offs

- **Probe traffic is non-zero overhead.** At 1 probe/sec per pair with 10 samples = 10 pps per pair. For 4 hosts pairwise = 12 pairs × 10 pps = 120 pps. Negligible.
- **Probe success doesn't prove all flows work** — only that ICMP works. TCP-specific issues (handshake, MSS clamping) need TCP probes. Add a `tcp_probe.py` if/when needed.
- **Pushgateway is single-replica.** If it goes down, probe data is lost during the outage. For real deployments use VictoriaMetrics' vmagent push-pull bridge or Mimir.

## See also

- `scripts/active_probe.py`
- `docs/slos.md` — Fabric-2 SLO would have a parallel "data plane availability" SLI built on `1 - fabric_probe_loss_ratio`
