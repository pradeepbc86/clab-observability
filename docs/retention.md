# Data retention policy

What we keep, for how long, and where it lives. Each store has its own retention semantics — `TTL` for ClickHouse, `--storage.tsdb.retention.time` for Prometheus, ILM for Elasticsearch, lifecycle policies for MinIO/Thanos.

## ClickHouse (BGP routes + BMP events)

| Table | TTL | Cold-tier transition | Reason |
|-------|-----|---------------------|--------|
| `bgp_routes` | 90 days delete | 30 days → cold volume | Daily ops typically look back 7-14d. 90d covers quarterly trending. |
| `bgp_peer_events` | 180 days | none | Peer-up/down infrequent; cheap to retain longer for forensics |
| `bgp_stats` | 90 days | none | Aggregated counters; older = aggregated rollup |
| `bgp_monitor_events` | 180 days | none | Init/term very infrequent |
| `rpki_invalid_observations` | 365 days | none | RPKI compliance investigations may span months |
| `bgp_routes_hourly_mv` | 365 days | 90d → cold | Materialized rollup for capacity planning over a year |
| `bgp_prefix_daily_churn_mv` | 90 days | none | Churn investigations are short-window |
| `bgp_routes_hourly` | 365 days | none | DAG-written rollup |
| `bgp_prefix_analytics` | 365 days | none | DAG-written summary |

TTLs configured in `clickhouse/schema.sql` via `TTL <column> + INTERVAL <n> DAY DELETE` clauses. Cold-tier transition requires a `storage_configuration` policy with a `cold` volume mounted on slow disk.

### Cold tier (lab-omitted)

```xml
<!-- /etc/clickhouse-server/config.d/storage.xml -->
<storage_configuration>
  <disks>
    <hot><path>/var/lib/clickhouse/hot/</path></hot>
    <cold><path>/var/lib/clickhouse/cold/</path></cold>
  </disks>
  <policies>
    <hot_cold>
      <volumes>
        <hot><disk>hot</disk></hot>
        <cold><disk>cold</disk></cold>
      </volumes>
    </hot_cold>
  </policies>
</storage_configuration>
```

Not configured in the lab (single-disk container). The schema's `TO VOLUME 'cold'` clause is documentation of intent.

## Prometheus

```yaml
# docker-compose.yml prometheus service command:
- '--storage.tsdb.retention.time=15d'
- '--storage.tsdb.retention.size=20GB'
```

15 days local. Anything older lives in Thanos → MinIO (object storage).

## Thanos / MinIO

Thanos sidecar uploads blocks to MinIO every 2h. MinIO bucket lifecycle policy keeps blocks per the policy below.

| Bucket | Retention | Resolution |
|--------|-----------|-----------|
| `thanos/raw` | 30 days | Original 15s resolution |
| `thanos/5m` | 1 year | Downsampled to 5-minute |
| `thanos/1h` | 5 years | Downsampled to 1-hour |

Thanos handles downsampling automatically via the `thanos-compactor` (not run in the lab — manual operation for now).

## Elasticsearch (syslog events + audit reports)

ILM policy `bgp-events-ilm` (not yet wired in lab):

| Phase | Duration | Action |
|-------|---------|--------|
| Hot | 7 days | Active writes, fast disk |
| Warm | 30 days | Read-only, cheaper disk |
| Cold | 90 days | Searchable but slow |
| Delete | 90+ | Removed |

Index pattern `bgp-events-YYYY.MM.DD` rolls daily.

## Airflow metadata DB

SQLite in the lab. Production: Postgres with retention of 90 days of DAG runs.

Airflow's `airflow db clean --clean-before-timestamp` is the operator hook.

## Compliance / audit logs

These ride on different rules:

- `compliance-drift.jsonl` (from `clab-automation/compliance.py`) — keep 1 year, weekly logrotate
- `audit.jsonl` (from `clab-ai-mcp/agent.py`) — keep 90 days, daily logrotate
- `deploy-events.jsonl` (from `clab-automation/deploy.py`) — keep 1 year

For real deployments these go to SIEM (Splunk HEC / Vector → S3 with object lock for tamper-resistance) and retention is governed by SOC2 / PCI / org policy.

## Why these numbers

- **90d for routes** — covers most ops investigations + one financial quarter
- **15d for raw Prometheus** — TSDB stays performant; long-term lives in Thanos
- **365d for materialized rollups** — capacity planning needs YoY comparison
- **180d for peer-events** — forensic investigations span weeks; cost is low
- **1y for RPKI invalid** — compliance reviews are annual

## When to change

- New compliance requirement (SOC2 audit asking for 13 months) → bump rollup TTL
- Disk pressure → shorten raw retention, lean on downsampled
- New use case (capacity planning over 3 years) → extend `bgp_routes_hourly_mv` TTL

## Verification

```bash
# Check ClickHouse TTL is actively pruning
docker exec clab-observability-clickhouse-1 clickhouse-client \
  --query "SELECT table, total_rows, total_bytes FROM system.parts WHERE active AND database='default'"

# Check Prometheus on-disk size
docker exec clab-observability-prometheus-1 du -sh /prometheus
```
