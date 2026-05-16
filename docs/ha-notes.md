# High-availability notes

Every service in this lab runs single-replica. This is intentional for a POC — but the design implications are real and worth documenting.

## Current topology (single-replica)

| Service | Failure mode | Recovery |
|---------|--------------|----------|
| OpenBMP collector | BMP feeds drop; FRR retries TCP every 5s | Restart container; FRR reconnects |
| ClickHouse | All BGP route data unavailable; writes buffer at OpenBMP for ~30s | Restart; OpenBMP flushes buffer |
| Elasticsearch | Syslog events unavailable; Logstash buffers UDP for ~10s | Restart; older events lost |
| Logstash | UDP/514 receiver down; events dropped silently | Restart; gap window |
| Prometheus | Metric collection halts; gaps in graphs | Restart; gap during outage |
| Thanos sidecar | Block upload to MinIO stops; local 15d retention covers | Restart; backfill occurs |
| MinIO | Thanos object store unavailable; sidecar buffers locally up to 2h | Restart; thanos resumes upload |
| Grafana | Dashboards unavailable; underlying data stays intact | Restart; no data loss |
| Airflow | DAGs miss scheduled runs | Restart; next interval picks up |
| Kibana | Event search UI down; ES data intact | Restart |

## What HA buys

| Pattern | What it solves |
|---------|----------------|
| Prometheus pair + Thanos query | One Prometheus can be restarted with no scrape gap; Thanos federates |
| ClickHouse 3-replica cluster | Loses ingest only if 2 replicas down simultaneously |
| Elasticsearch 3+ master nodes + 2+ data nodes | Tolerates 1 master + 1 data loss |
| MinIO 4-node erasure-coded | Tolerates 1 node loss with no data loss |
| Airflow Celery+Redis | DAG dispatch survives scheduler restart |

## What this lab models

Since we run single-replica, the "HA story" is: **everything is recoverable**, **nothing is unrecoverable**, and **gaps are bounded** by the buffer in the upstream service.

- OpenBMP's TCP retry behavior means we don't lose route data if ClickHouse blips for <30s
- Logstash UDP receiver loses any events arriving during its restart window
- Prometheus loses scrape data for its restart window
- Thanos covers long-term metric durability via MinIO

## What we'd change for fleet scale

| Service | Move to |
|---------|---------|
| OpenBMP | Cluster behind LB; primary + standby with route-replication between |
| ClickHouse | 3 replicas with `Distributed` engine table on top |
| Elasticsearch | 3 master / 3+ data with replicas: 1 |
| Prometheus | 2× per region with the same scrape config; Thanos query layer dedups |
| Thanos | Receive-mode for push ingestion + multi-replica querier |
| MinIO | 4-node distributed mode, erasure coding |
| Airflow | Celery executor + Redis broker + Postgres metadata DB; 2× scheduler |
| Grafana | 2× stateless replicas behind LB; Postgres for dashboards |
| Alertmanager | 3-node cluster for de-dup and quorum |

## Backup posture

| Service | Backup | Frequency |
|---------|--------|-----------|
| ClickHouse | `BACKUP` to S3 + WAL ship | daily |
| Elasticsearch | Snapshot repository → S3 | daily |
| Prometheus | Thanos-uploaded blocks (already backed up by design) | continuous |
| Airflow metadata | `pg_dump` → S3 | hourly |
| Grafana | Dashboard JSON → Git (provisioned dashboards already in repo) | n/a |

Lab omits these — single-disk container, no backup target.

## Documenting RPO/RTO

This is what a real on-call doc would commit to:

| Service | RPO | RTO |
|---------|-----|-----|
| Route data (BGP routes) | 30s (OpenBMP buffer) | 5min (restart container) |
| Event data (syslog) | best-effort UDP | n/a (events lost during outage) |
| Metric data | 15s scrape interval | 2min (Prometheus restart) |
| Long-term metrics | 2h (Thanos block upload) | 5min |
| Dashboards | 0 (provisioned from Git) | 1min |

## When this matters

- Customer-facing SLA promises retention + recovery time
- Regulatory (SOC2, PCI) requires documented backup + recovery
- Incident review: "how long was the gap?" needs an answer

For a lab portfolio: explicitly stating RPO/RTO trade-offs in `ha-notes.md` is the senior signal. Implementing the HA topology is the principal signal — out of scope here, but the design path is clear.
