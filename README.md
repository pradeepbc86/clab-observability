# clab-obs-telemetry

Full BGP observability stack on ContainerLab: BMP route analytics in ClickHouse, syslog events in ELK, long-term metrics with Prometheus + Thanos, Airflow DAGs for scheduled audits, all visualized in Grafana.

## Architecture

```
ContainerLab FRR nodes (3-node lab)
  ├── BMP (port 11019) → OpenBMP → ClickHouse → Grafana (route analytics)
  │   (BGP route monitoring, prefix churn, top peers)
  │
  ├── Syslog (UDP 514) → Logstash → Elasticsearch → Kibana (events)
  │   (Neighbor up/down, prefix changes, BGP state events)
  │
  ├── frr_exporter → Prometheus → Thanos → Grafana (long-term metrics)
  │   (BGP neighbor count, route counts, convergence times)
  │
  └── Airflow → ClickHouse + Cloudflare RPKI → Elasticsearch
      (Daily capacity reports, weekly route audits with RPKI checks)
```

## Tools

| Tool | Purpose |
|------|---------|
| **ContainerLab** | 3-node FRR lab with BMP + syslog enabled |
| **FRRouting** | BGP control plane with BMP support |
| **OpenBMP** | BGP Monitoring Protocol collector |
| **ClickHouse** | Time-series DB for BGP route analytics |
| **Elasticsearch** | Syslog event storage |
| **Logstash** | Parse FRR syslog events |
| **Kibana** | Syslog event visualization |
| **Prometheus** | Metrics collection (frr_exporter) |
| **Thanos** | Long-term Prometheus storage |
| **Grafana** | Unified dashboard (ClickHouse + Prometheus + ES) |
| **MinIO** | Object storage for Thanos |
| **Airflow** | Scheduled DAGs (daily capacity report, weekly route audit) |
| **GitLab CI/CD** | Docker Compose health checks |

## Dashboards

- **BGP Routes** (ClickHouse): Prefix count over time, churn rate, top peers
- **BGP Events** (Elasticsearch): Neighbor flap timeline, state changes
- **BGP Metrics** (Prometheus/Thanos): Neighbor count, route counts, convergence

## Airflow DAGs

- **`bgp_capacity_report`** (daily): Pulls 24h prefix counts and top peers from ClickHouse, publishes summary to Elasticsearch
- **`bgp_route_audit`** (weekly, Mondays): Detects churn anomalies (>10 state changes/24h), surfaces new prefixes not seen the prior week, spot-checks top 20 prefixes against Cloudflare RPKI

## Quick Start

```bash
# Deploy lab
make deploy

# Start monitoring stack
docker-compose up -d

# Access dashboards
# Grafana:   http://localhost:3000 (admin/admin)
# Kibana:    http://localhost:5601
# ClickHouse: http://localhost:8123

# Validate
docker-compose logs -f

# Cleanup
make destroy
docker-compose down
```

## Files

- `docker-compose.yml` — All observability services (OpenBMP, ClickHouse, ELK, Prometheus, Thanos, Grafana, MinIO)
- `topology/lab.clab.yml` — 3-node FRR lab with BMP + syslog
- `configs/frr/frr.conf` — FRR config with BMP neighbor + syslog
- `configs/logstash/bgp-syslog.conf` — Parse FRR BGP events
- `configs/prometheus/prometheus.yml` — Scrape frr_exporter
- `configs/thanos/store.yml` — Thanos sidecar config
- `clickhouse/schema.sql` — BGP route tables
- `grafana/dashboards/` — Pre-built dashboards (JSON)

## Network Setup Note

ContainerLab creates its own Docker bridge network (`clab-<topo>`). The observability stack runs in Docker Compose's default bridge. For BMP, syslog, and frr_exporter to reach OpenBMP/Logstash/Prometheus, both networks must be connected.

Option 1 — join the clab network in docker-compose:
```yaml
networks:
  default:
    external: true
    name: clab-obs-telemetry
```

Option 2 — run everything with `--network host` (Linux only).

Option 3 — use Docker host IP (`172.17.0.1`) as the collector address (already set in `configs/frr/spine1.conf`).

## Prerequisites

- Docker & Docker Compose
- ContainerLab
- 4GB+ RAM (all services running)
- FRRouting image: `frrouting/frr:9.1.0`

## Data Flow

1. **Route Analytics (ClickHouse)**
   - FRR sends BMP updates → OpenBMP → ClickHouse
   - Grafana queries `bgp_routes` table for prefix trends, churn, top peers
   - Example: "Show prefix count per AS over 24 hours"

2. **Event Logs (Elasticsearch)**
   - FRR logs syslog (BGP neighbor state changes)
   - Logstash parses `%BGP-5-ADJCHANGE` messages
   - Kibana timeline shows neighbor flaps per hour
   - Example: "When did neighbor 10.1.1.1 change state?"

3. **Long-term Metrics (Prometheus/Thanos)**
   - frr_exporter exposes FRR metrics
   - Prometheus scrapes → Thanos stores in MinIO
   - Grafana shows 30-day trends on neighbor count, route count
   - Example: "BGP convergence time trending up over time?"
