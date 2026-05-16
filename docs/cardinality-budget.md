# Cardinality budget

Prometheus failure mode #1 is **cardinality explosion** — a label dimension whose value space grows unboundedly. The TSDB head series count grows linearly with cardinality; memory grows ~3-5KB per series. At ~1M series Prometheus needs 4-5GB RAM and queries get slow; at 10M+ it falls over.

This document defines what we expect each metric to contribute and what we monitor.

## Metric inventory

| Metric | Expected cardinality | Driven by |
|--------|---------------------:|-----------|
| `frr_bgp_peer_state` | ~4 peers × 4 nodes × 6 states = 96 | Peer count, state enum |
| `frr_bgp_peer_prefix_received_count` | ~8 (peer × node) | Same |
| `frr_bfd_peer_uptime_seconds` | ~8 | Same |
| `frr_bfd_detect_time_seconds_bucket` | ~8 × 10 buckets = 80 | Histogram |
| `frr_evpn_vni_active` | ~1 VNI × 2 leaves = 2 | Tenant VNI count |
| `up` | ~7 targets | Scrape job × instance |
| Total expected | **~200 series** | Lab scale |

For comparison, a 1000-leaf fabric would project to ~50k-200k series — still well within Prometheus limits, but the scaling axis is `peers × nodes`. Adding labels like `prefix` or `path_id` would multiply by 100k+.

## Hard limits (Prometheus settings)

```yaml
# In docker-compose.yml, prometheus command line:
- '--storage.tsdb.head-series-limit=500000'   # refuse new series past 500k
- '--query.max-samples=50000000'              # 50M samples per query
- '--query.timeout=60s'
```

## Alerts for cardinality health

Already in `configs/prometheus/alerts.yml` (not the BGP alerts; this is meta):

```yaml
- alert: PrometheusHighSeriesCount
  expr: prometheus_tsdb_head_series > 400000
  for: 10m
  labels: { severity: warning, team: observability }
  annotations:
    summary: "TSDB series count {{ $value }} approaching limit"
```

```yaml
- alert: PrometheusSeriesGrowthSpike
  expr: rate(prometheus_tsdb_head_series_created_total[5m]) > 100
  for: 10m
  labels: { severity: warning, team: observability }
  annotations:
    summary: "Series creation rate {{ $value }}/s — investigate new label dimension"
```

## What blows up Prometheus

| Anti-pattern | Why bad | Fix |
|--------------|---------|-----|
| `label_with_uuid` | Unbounded | Drop the label via `metric_relabel_configs` |
| `prefix="1.2.3.4/24"` as a metric label | 800k prefix space | Use ClickHouse; Prometheus is wrong tool |
| `path_id` from BGP add-path | Per-flow unbounded | Aggregate at exporter |
| `customer_email` | Bonkers but happens | Drop it |
| Free-text version strings | Bounded but high | Use `info` metric pattern + relabel |
| Histogram with too many buckets | ×N per bucket | Pick 8-12 buckets that match SLO thresholds |

## Drop rules at scrape time

`configs/prometheus/prometheus.yml` (not yet wired — example):

```yaml
scrape_configs:
  - job_name: 'frr-fabric'
    metric_relabel_configs:
      # Drop any metric that has a 'prefix' label — wrong store
      - source_labels: [prefix]
        action: drop
        regex: .+
      # Strip 'pid' / 'instance_uuid' from any metric
      - regex: '(pid|instance_uuid)'
        action: labeldrop
```

## ClickHouse cardinality (parallel concern)

ClickHouse handles high cardinality natively via `LowCardinality(String)` columns — used in `schema.sql` for `action`, `monitor_ip`, `rpki_status`. These dictionary-encode the column when unique values are <10k, giving the storage of a UInt8 with the API of a String.

Don't `LowCardinality` columns that *are* high-cardinality (like `prefix` itself) — that's worse than plain String.

## When to revisit

- After every new exporter is added — re-run the inventory
- When a query starts slowing down without obvious cause — check `topk(20, count by (__name__) ({__name__=~".+"}))`
- Quarterly: review the highest-cardinality metrics and validate they still earn their cost
