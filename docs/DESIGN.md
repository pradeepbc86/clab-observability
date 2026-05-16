# clab-observability — Design Document

> **Audience:** A network engineer who knows BGP and has seen Prometheus/Grafana dashboards but hasn't built the data pipeline behind them. You may have heard of BMP, OpenBMP, ClickHouse, ELK, Thanos, Airflow — this doc explains what each one *is*, why it's here, and how they fit into a coherent observability stack.

---

## 1. What this repo is

A **three-tier observability stack** for a BGP fabric, plus Airflow DAGs for scheduled analytics. The three tiers exist because no single database is good at all three workloads:

| Data shape | Backend | Why |
|------------|---------|-----|
| Wide route data (BMP route-monitoring) | **ClickHouse** | Columnar, handles millions of prefix-rows efficiently |
| Free-text events (syslog) | **Elasticsearch + Logstash + Kibana** | Full-text search, schema-flexible |
| Numeric metrics (BGP state, BFD detect, prefix counts) | **Prometheus + Thanos + MinIO** | TSDB optimized for time-series scrape model |

Plus:
- **Grafana** unifies all three as a viewing layer
- **Airflow** runs scheduled DAGs (daily capacity report, weekly route audit with RPKI spot-checks)
- **Alertmanager** routes 8 alerts (5 fabric health + 3 platform health) to severity-appropriate receivers
- **Pushgateway** for short-lived job pushes (active OAM probes)

This isn't an "all-in-one solution" because production observability never is. Each store specializes; Grafana federates the view.

---

## 2. Mental model — what flows where

```
                       ┌─────────────────────────────────┐
                       │  clab-fabric-evpn (the network) │
                       └────┬─────────┬──────────────┬───┘
                            │         │              │
                       BMP/11019  syslog/UDP-514   metrics/9342
                            │         │              │
                            ▼         ▼              ▼
                    ┌──────────┐  ┌────────┐  ┌──────────────┐
                    │ OpenBMP  │  │Logstash│  │ Prometheus   │
                    └────┬─────┘  └───┬────┘  └──────┬───────┘
                         │            │              │
                         ▼            ▼              ▼ (2h block)
                    ┌──────────┐  ┌──────────┐  ┌──────────────┐
                    │ClickHouse│  │ Elastic  │  │ Thanos sidecar│
                    │bgp_routes│  │bgp-events│  │              │
                    │bgp_peer_ │  │          │  └──────┬───────┘
                    │  events  │  │          │         │ S3 PUT
                    │bgp_stats │  │          │         ▼
                    └────┬─────┘  └────┬─────┘  ┌──────────────┐
                         │             │        │    MinIO     │
                         │             │        └──────────────┘
                         └──────────┬──┴───────────────┐
                                    │                  │
                                    ▼                  ▼
                              ┌──────────────────────────────┐
                              │          Grafana             │
                              │  (3 datasources, dashboards) │
                              └──────────────────────────────┘
                                            ▲
                                            │ queries / events
                                            │
                              ┌──────────────────────────────┐
                              │ Airflow DAGs                 │
                              │  - bgp_capacity_report (daily)│
                              │  - bgp_route_audit (weekly)  │
                              └──────────────────────────────┘
                                            ▲
                                            │ pushes
                              ┌─────────────┴────────────────┐
                              │ pushgateway (active probes)  │
                              │ scrape source for Prometheus │
                              └──────────────────────────────┘
                                            ▲
                                            │
                              ┌──────────────────────────────┐
                              │ Alertmanager (5+3 alerts)    │
                              │  - severity routing          │
                              │  - inhibit_rules for dedup   │
                              │  - 3 receiver classes:       │
                              │    default-log / page-noc /  │
                              │    ticket-noc                │
                              └──────────────────────────────┘
```

Every arrow is a real data flow. Bridges between the clab Docker network (where the fabric lives) and the docker-compose network (where this stack lives) are documented in the README under "Network Setup Note."

---

## 3. Tools used — what they are and why

### 3.1 BMP (RFC 7854) and OpenBMP

**What BMP is:** BGP Monitoring Protocol. A protocol where a BGP speaker (your router) opens a TCP connection to a collector and *streams* its BGP state — every route received, every peer up/down, periodic stats. Distinct from `show bgp`: BMP is push-based and complete, including the pre-policy Adj-RIB-In which `show bgp` doesn't expose.

**Five message types** (we model all of them in the ClickHouse schema):
1. **Route Monitoring** — every BGP UPDATE the peer received (announce / withdraw)
2. **Peer Up** — session came up, includes capabilities exchanged
3. **Peer Down** — session went down, includes reason code
4. **Statistics Report** — periodic counters (prefixes-rejected, AS-path loops, etc.)
5. **Initiation / Termination** — collector handshake messages

**OpenBMP collector:** An open-source BMP collector that listens on TCP/11019, accepts BMP streams from routers, parses the messages, and ships parsed records to Kafka or directly to a database. We use it as the BMP → ClickHouse bridge.

**Why this matters:** With BMP you can answer "what did router X see at time T?" — including routes it received but didn't install due to policy. Without BMP, you only see what `show bgp` reveals, which is post-policy.

### 3.2 ClickHouse

**What it is:** A columnar OLAP database. Each "row" is logical; physically the data is stored column-by-column, compressed per column. Result: massive compression ratios (10-50x) and analytical queries that scan only the columns they need.

**Why for BMP data:** BGP route data is *wide* (prefix, AS-path array, communities, peer, monitor, timestamps) and *high-cardinality* (millions of distinct prefixes). ClickHouse's `LowCardinality(String)` dictionary-encodes columns with < 10k unique values; `Array(UInt32)` natively handles AS-paths.

**Tables we model** (`clickhouse/schema.sql`):

| Table | What it holds | Retention |
|-------|---------------|-----------|
| `bgp_routes` | Every BMP route-monitoring message (announce/withdraw) | 90 days |
| `bgp_peer_events` | Peer up/down events | 180 days |
| `bgp_stats` | BMP statistics reports (counters) | 90 days |
| `bgp_monitor_events` | Collector init/term | 180 days |
| `rpki_invalid_observations` | Prefixes that failed RPKI ROV | 365 days |

Plus two **MaterializedViews** populating automatically from `bgp_routes`:

- `bgp_routes_hourly_mv` — hourly prefix counts per peer (rollup for capacity dashboards)
- `bgp_prefix_daily_churn_mv` — per-prefix state-change counts per day

**Why MaterializedViews:** Hot dashboard queries (e.g., "prefix count over time") shouldn't scan the 100M-row raw table every refresh. The MV is populated at insert time; dashboards read pre-aggregated data.

**Partitioning + TTL:** Every table has `PARTITION BY toYYYYMM(timestamp)` and `TTL <column> + INTERVAL <n> DAY DELETE`. ClickHouse automatically drops partitions past the TTL — disk doesn't grow forever.

### 3.3 Elasticsearch + Logstash + Kibana (ELK)

**What ELK is:** A log-storage and search system.

- **Logstash** — accepts logs via various inputs (UDP syslog in our case), parses them with grok patterns, ships to Elasticsearch
- **Elasticsearch** — distributed search engine; full-text search, aggregations
- **Kibana** — web UI for searching ES, building dashboards

**Why for syslog:** Network device syslog is free-text with semi-structured patterns (`%BGP-5-ADJCHANGE: neighbor X.X.X.X (Y) is up/down`). Storing this in a TSDB doesn't work; storing it in a relational DB requires you to know the schema upfront. ES indexes the text and lets you query by any field.

**Our Logstash config** (`configs/logstash/bgp-syslog.conf`):

```
input { udp { port => 514 type => "syslog" } }

filter {
  grok {
    match => { "message" => "%{SYSLOGTIMESTAMP:syslog_timestamp} ..." }
  }
  if "ADJCHANGE" in [syslog_message] {
    grok {
      match => { "syslog_message" => "BGP-5-ADJCHANGE: neighbor %{IP:neighbor_ip} ..." }
    }
  }
  date { match => [ "syslog_timestamp", ... ] }
}

output {
  elasticsearch { index => "bgp-events-%{+YYYY.MM.dd}" }
}
```

Parse the syslog format → extract `neighbor_ip`, `bgp_state` → store in daily-rotated ES index.

### 3.4 Prometheus + Thanos + MinIO

**Prometheus:** Pull-based time-series database. It *scrapes* `/metrics` endpoints on a schedule (every 15s here), stores the samples in a local TSDB. Local retention is 15 days (`--storage.tsdb.retention.time=15d`).

**Thanos sidecar:** Runs alongside Prometheus. Every 2 hours, it uploads Prometheus's completed TSDB blocks to S3-compatible storage. Long-term retention without bloating Prometheus's local disk.

**MinIO:** S3-compatible object storage running locally. Provides the S3 endpoint Thanos uploads to. In production this would be actual S3 / GCS / Azure Blob; MinIO is the lab equivalent.

**Why this stack and not just Prometheus:** Prometheus alone can't do long-term storage cheaply. Thanos extends it: 15d hot data in Prom, 1y+ cold data in object storage, queried transparently via `thanos query`.

**What we scrape:**
- `prometheus` itself (self-monitoring)
- `frr-fabric` — frr_exporter on each FRR node (port 9342)
- `thanos` sidecar metrics (port 10902)
- `pushgateway` — short-lived job pushes (active OAM probes, batch DAG metrics)

### 3.5 Grafana

**What it is:** Web UI for dashboards. Connects to multiple datasources (Prometheus, ClickHouse, ES) and lets you build panels that query them.

**How datasources are wired:** `grafana/datasources/datasources.yml` declares three datasources provisioned at startup — no manual UI clicking needed. Grafana reads this on boot and auto-configures.

**Pre-built dashboards** (`grafana/dashboards/`):
- `bgp-routes.json` — ClickHouse-backed: prefix count over time, churn, top peers
- `bgp-events.json` — Elasticsearch-backed: neighbor flap timeline
- `bgp-metrics.json` — Prometheus-backed: BGP neighbor state, BFD detect, EVPN VNI

### 3.6 Airflow

**What it is:** A DAG-based scheduler. You write Python files defining Directed Acyclic Graphs of tasks; Airflow runs them on cron-like schedules, tracks success/failure, retries, alerts.

**Why we use it:** Some analytics aren't reactive (alerts on metrics) — they're periodic (daily capacity report, weekly route audit). Cron + bash works at small scale; Airflow handles dependencies, retries, monitoring at fleet scale.

**Two DAGs ship:**

#### `bgp_capacity_report` (daily, @06:00)

```
query_prefix_counts → query_top_peers → publish_report
                          (both run in parallel; publish waits for both)
```

- `query_prefix_counts` — ClickHouse query: `SELECT toStartOfHour(timestamp), countIf(withdrawn=0) FROM bgp_routes WHERE timestamp >= now() - INTERVAL 24 HOUR GROUP BY hour`
- `query_top_peers` — Top 10 peers by route count last 24h
- `publish_report` — POST the summary doc to Elasticsearch index `capacity-reports`

XCom (Airflow's task-to-task data passing) carries the query results from the upstream tasks to the publisher.

#### `bgp_route_audit` (weekly, Mondays @08:00)

```
detect_churn_anomalies ──┐
detect_new_prefixes ─────┼─► publish_audit
validate_rpki ───────────┘
```

- `detect_churn_anomalies` — Flag prefixes with >10 state changes in 24h (flapping)
- `detect_new_prefixes` — Prefixes seen this week but not the prior week
- `validate_rpki` — Spot-check top 20 prefixes against Cloudflare RPKI API
- `publish_audit` — POST findings to ES `route-audits` index

Airflow's `LocalExecutor` + SQLite metadata DB is fine for two DAGs at lab scale; production scales to `CeleryExecutor` + Postgres + Redis.

### 3.7 Alertmanager

**What it is:** Receives alerts from Prometheus, **routes** them by labels, dedupes, inhibits, and ships them to receivers (PagerDuty, Slack, email, webhooks).

**Why it's separate from Prometheus:** Prometheus *evaluates* alert rules and fires; Alertmanager handles the routing/dedup/notification logic. Separation of concerns.

**Our routing** (`configs/alertmanager.yml`):

```yaml
route:
  receiver: default-log
  group_by: ['alertname', 'team']

  routes:
    - matchers: [severity = "critical", team = "network-ops"]
      receiver: page-noc          # PagerDuty in production
    - matchers: [severity = "warning", team = "network-ops"]
      receiver: ticket-noc        # Slack/Jira in production

inhibit_rules:
  - source_matchers: [severity = "critical"]
    target_matchers: [severity = "warning"]
    equal: ['alertname', 'instance']
```

**Inhibit rule:** if a `critical` is firing for the same `alertname + instance`, suppress matching `warning` alerts. Avoids alert storms when one failure cascades.

### 3.8 Pushgateway

**What it is:** A Prometheus component for *short-lived* jobs that can't be scraped (because they don't run long enough). Jobs POST metrics to Pushgateway; Prometheus scrapes Pushgateway.

**Why we need it:** The active OAM probes (`scripts/active_probe.py`) run once per minute via cron from inside a netshoot container. They don't run a `/metrics` endpoint — they're transient processes. Pushgateway is the bridge.

### 3.9 Active OAM probes

**What they are:** Synthetic data-plane tests. The script runs `ping` from h1 to h2 (small + DF-bit jumbo), parses the output, pushes results to Pushgateway as Prometheus metrics.

**Metrics emitted:**
- `fabric_probe_rtt_avg_ms{source, target}` — average RTT
- `fabric_probe_loss_ratio{source, target}` — 0.0–1.0 packet loss
- `fabric_probe_pmtu_1500_ok{source, target}` — 1 if 1500B DF passes
- `fabric_probe_pmtu_9000_ok{source, target}` — 1 if 9000B DF passes

**Why this matters — silent blackhole detection:** Control plane can look 100% green (BGP Established, BFD up, FIB programmed) while packets are dropped due to a netfilter rule, FIB inconsistency, or VXLAN encap state corruption. Active probes generate ground truth. The alert `DataPlaneSilentBlackhole` fires when `fabric_probe_loss_ratio > 0.5` — control plane is healthy but packets aren't moving.

This closes a class of failure that pure control-plane monitoring **cannot** detect.

---

## 4. Repository structure

```
clab-observability/
├── .github/workflows/
│   └── ci.yml                       # yamllint + compose-validate + promtool check
├── airflow/
│   └── dags/
│       ├── capacity_report.py       # Daily DAG
│       └── route_audit.py           # Weekly DAG
├── clickhouse/
│   └── schema.sql                   # All 5 BMP message types + MVs + analytics tables
├── configs/
│   ├── alertmanager.yml             # Routing + inhibit_rules + receivers
│   ├── frr/                         # Per-node FRR configs (3-node telemetry lab)
│   │   ├── daemons
│   │   ├── leaf1.conf
│   │   ├── leaf2.conf
│   │   └── spine1.conf
│   ├── logstash/
│   │   └── bgp-syslog.conf          # UDP/514 input → grok parse → ES output
│   ├── prometheus/
│   │   ├── alerts.yml               # 8 alert rules
│   │   └── prometheus.yml           # Scrape configs + Alertmanager wiring
│   └── thanos/
│       └── store.yml                # Thanos sidecar S3 config
├── docs/
│   ├── DESIGN.md                    # THIS FILE
│   ├── slos.md                      # Fabric + pipeline SLOs (multi-window alerts)
│   ├── cardinality-budget.md        # Metric inventory + drop rules
│   ├── retention.md                 # Per-table TTL with rationale
│   ├── ha-notes.md                  # Single-replica failure modes + RPO/RTO
│   ├── active-oam.md                # Why active probes matter
│   └── adr/
│       ├── 0001-clickhouse-not-timescale.md
│       ├── 0002-three-layer-observability.md
│       └── 0003-airflow-not-cron.md
├── grafana/
│   ├── dashboards/
│   │   ├── bgp-events.json
│   │   ├── bgp-metrics.json
│   │   └── bgp-routes.json
│   └── datasources/
│       └── datasources.yml          # Auto-provision Prometheus + ClickHouse + ES
├── queries/
│   ├── README.md                    # Index of example queries
│   ├── clickhouse/
│   │   ├── bgp_analytics.sql        # Top flappers, peer counts, anycast verification
│   │   └── rpki.sql                 # RPKI invalid history + ROA coverage
│   ├── elasticsearch/
│   │   └── bgp_events.txt           # Neighbor flap timeline DSL queries
│   └── prometheus/
│       ├── bgp_state.promql         # BGP/BFD/EVPN PromQL gallery
│       └── cardinality.promql       # Cardinality budget audits
├── runbooks/                        # One markdown per alert
│   ├── bfd-flap.md
│   ├── bgp-neighbor-down.md
│   ├── bgp-prefix-drop.md
│   ├── evpn-vni-down.md
│   └── rpki-invalid.md
├── scripts/
│   └── active_probe.py              # ICMP + PMTU probes → pushgateway
├── topology/
│   └── lab.clab.yml                 # 3-node FRR lab (telemetry sources)
├── .env.example
├── .gitattributes                   # Suppress Linguist detection
├── .gitignore
├── .gitlab-ci.yml                   # Legacy GitLab CI
├── .pre-commit-config.yaml
├── LICENSE                          # MIT
├── Makefile                         # deploy / destroy / validate / monitor
├── README.md
├── SECURITY.md
└── docker-compose.yml               # The full observability stack
```

### 4.1 What each directory does

| Directory | Role |
|-----------|------|
| `airflow/dags/` | Python DAG definitions — scheduled analytics |
| `clickhouse/` | Database schema applied at first boot |
| `configs/` | Per-service config files mounted into containers |
| `docs/` | All long-form documentation: SLOs, cardinality budget, retention policy, HA notes, active-OAM, ADRs |
| `grafana/` | Dashboards (JSON) and datasource provisioning |
| `queries/` | Working-example queries for each datasource — operator reference, not executed at runtime |
| `runbooks/` | One markdown per alert: symptoms → diagnosis → mitigation → recovery validation |
| `scripts/` | Helper scripts (currently the active probe) |

---

## 5. Walking through `docker-compose.yml`

This is the orchestration file that brings up the entire stack with `docker-compose up -d`.

```yaml
services:
  openbmp:
    image: openbmp/openbmp:latest
    ports:
      - "11019:11019"
    environment:
      MR_SERVICE: "kafka:9092"
```

**OpenBMP**, listening on TCP/11019 for incoming BMP streams from routers.

```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "127.0.0.1:8123:8123"
    volumes:
      - ./clickhouse/schema.sql:/docker-entrypoint-initdb.d/schema.sql
      - clickhouse_data:/var/lib/clickhouse
```

**ClickHouse server**, bound to `127.0.0.1:8123` (loopback only for security). The schema file is mounted into the special init directory — ClickHouse runs every SQL file there on first boot, creating our tables and MVs.

```yaml
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.0.0
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
    ports:
      - "9200:9200"
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data
```

**Elasticsearch**, single-node mode (no cluster discovery), security disabled (lab only — production would have TLS + auth).

```yaml
  logstash:
    image: docker.elastic.co/logstash/logstash:8.0.0
    volumes:
      - ./configs/logstash/bgp-syslog.conf:/usr/share/logstash/pipeline/logstash.conf
    ports:
      - "514:514/udp"
    depends_on:
      - elasticsearch
```

**Logstash**, accepting UDP/514 syslog. The pipeline config is mounted in; Logstash auto-reloads on file change in newer versions.

```yaml
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./configs/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./configs/prometheus/alerts.yml:/etc/prometheus/alerts.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=15d'
      - '--storage.tsdb.retention.size=20GB'
      - '--storage.tsdb.head-series-limit=500000'
      - '--web.enable-lifecycle'
    ports:
      - "9090:9090"
    depends_on:
      - alertmanager
```

**Prometheus**, with explicit retention flags:
- `--storage.tsdb.retention.time=15d` — keep 15 days local
- `--storage.tsdb.retention.size=20GB` — also cap by disk usage
- `--storage.tsdb.head-series-limit=500000` — cardinality budget; refuse new series past 500k
- `--web.enable-lifecycle` — enables `POST /-/reload` for config reload without restart

```yaml
  alertmanager:
    image: prom/alertmanager:latest
    volumes:
      - ./configs/alertmanager.yml:/etc/alertmanager/alertmanager.yml
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
    ports:
      - "127.0.0.1:9093:9093"
```

**Alertmanager**, bound to loopback only.

```yaml
  pushgateway:
    image: prom/pushgateway:latest
    ports:
      - "127.0.0.1:9091:9091"
```

**Pushgateway**, accepts pushes on `:9091`.

```yaml
  thanos:
    image: quay.io/thanos/thanos:latest
    command: sidecar --tsdb.path=/prometheus
    volumes:
      - prometheus_data:/prometheus
    ports:
      - "10902:10902"
```

**Thanos sidecar**, sharing the `prometheus_data` volume so it can see Prometheus's blocks. Listens on `:10902` for the Thanos query API.

```yaml
  minio:
    image: minio/minio:latest
    ports:
      - "127.0.0.1:9000:9000"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minioadmin}
    volumes:
      - minio_data:/data
    command: server /data
```

**MinIO** (S3-compatible object storage). Credentials from env vars with `:-default` fallback.

```yaml
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: ${GF_SECURITY_ADMIN_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GF_SECURITY_ADMIN_PASSWORD:-admin}
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/datasources:/etc/grafana/provisioning/datasources
    depends_on:
      - prometheus
      - clickhouse
      - elasticsearch
```

**Grafana**, with dashboards and datasources auto-provisioned from the bind-mounted directories.

```yaml
  airflow:
    image: apache/airflow:2.9.0
    ports:
      - "127.0.0.1:8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      _AIRFLOW_DB_MIGRATE: "true"
      _AIRFLOW_WWW_USER_CREATE: "true"
      _AIRFLOW_WWW_USER_USERNAME: ${AIRFLOW_USER:-admin}
      _AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_PASSWORD:-admin}
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - airflow_data:/opt/airflow
    command: standalone
    depends_on:
      - clickhouse
      - elasticsearch
```

**Airflow standalone mode** — single process with SQLite metadata DB. DAGs mounted from the repo. The `standalone` command runs the scheduler + webserver + workers in one process. Fine for the lab; production would split into separate services with Celery + Postgres + Redis.

---

## 6. Walking through the ClickHouse schema

`clickhouse/schema.sql`. Highlights:

### 6.1 The hot table — `bgp_routes`

```sql
CREATE TABLE IF NOT EXISTS default.bgp_routes
(
    timestamp    DateTime,
    prefix       String,
    origin_as    UInt32,
    peer_ip      String,
    peer_asn     UInt32,
    action       LowCardinality(String),
    path         Array(UInt32),
    communities  Array(String),
    withdrawn    UInt8,
    monitor_ip   LowCardinality(String),
    rpki_status  LowCardinality(String) DEFAULT 'unknown'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, prefix, peer_asn)
TTL timestamp + INTERVAL 90 DAY DELETE,
    timestamp + INTERVAL 30 DAY TO VOLUME 'cold'
SETTINGS index_granularity = 8192;
```

**Engine choice — MergeTree:** ClickHouse's standard analytical engine. Data is stored in sorted parts that are periodically merged. Fast inserts (millions/sec), fast range scans on the ORDER BY columns.

**`PARTITION BY toYYYYMM(timestamp)`:** Each month gets its own physical partition. Queries with `WHERE timestamp >= 'YYYY-MM-XX'` skip irrelevant partitions entirely.

**`ORDER BY (timestamp, prefix, peer_asn)`:** The sort order within each partition. This is also the *primary index* — queries filtering on these columns hit the sparse index.

**`TTL timestamp + INTERVAL 90 DAY DELETE`:** Automatically drop rows older than 90 days. ClickHouse runs the TTL check during background merges.

**`TO VOLUME 'cold'`:** After 30 days, move parts to a 'cold' storage volume (slower disk). Requires a `storage_configuration` policy in ClickHouse config (documented in `docs/retention.md`).

**`LowCardinality(String)`:** Dictionary-encoded columns. ClickHouse maintains a per-part dictionary of unique values; the column stores `UInt8/16` indices. For columns with <10k unique values (action, monitor_ip, rpki_status), this gives 100-1000x compression.

**`Array(UInt32)`:** AS-path stored natively as an array. Query with `length(path)`, `arrayElement(path, 1)`, etc.

### 6.2 Materialized views — the rollups

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS default.bgp_routes_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, peer_asn)
TTL hour + INTERVAL 365 DAY DELETE
AS SELECT
    toStartOfHour(timestamp) AS hour,
    peer_asn,
    countIf(withdrawn = 0) AS prefix_count,
    countIf(withdrawn = 1) AS withdrawn_count,
    count() AS total_events
FROM default.bgp_routes
GROUP BY hour, peer_asn;
```

**SummingMergeTree:** When ClickHouse merges parts of this MV, rows with the same `ORDER BY` key are *summed*. So multiple `(hour=10, peer_asn=4200000001, prefix_count=42)` rows merge into one.

**Triggered at insert time:** Whenever a row is inserted into `bgp_routes`, ClickHouse evaluates the MV's SELECT for that row and inserts into the MV's table. The dashboard reads from the small rollup table, not the raw `bgp_routes`.

**Net effect:** Hourly capacity dashboards refresh in < 100ms instead of minutes.

---

## 7. Walking through the alerts

`configs/prometheus/alerts.yml` — 8 alerts in 3 groups.

### 7.1 `bgp_fabric_health` (5 alerts)

```yaml
- alert: BGPNeighborDown
  expr: frr_bgp_peer_state{state!="Established"} == 1
  for: 1m
  labels:
    severity: critical
    team: network-ops
  annotations:
    summary: "BGP neighbor {{ $labels.peer }} on {{ $labels.instance }} is not Established"
    description: |
      Peer {{ $labels.peer }} has been in state {{ $labels.state }} for at least 1 minute.
    runbook_url: "https://github.com/pradeepbc86/clab-observability/blob/main/runbooks/bgp-neighbor-down.md"
```

**The `expr` semantics:** `frr_bgp_peer_state` is a metric with a `state` label. The exporter emits one time series per peer per state, where value 1 = currently in that state, 0 = not. The expression matches series where state ≠ Established AND value = 1. Result is one alert per (instance, peer) currently down.

**`for: 1m`:** The condition must hold continuously for 1 minute before firing. Suppresses transient flaps.

**`runbook_url`:** Annotation that Alertmanager / PagerDuty / Grafana surfaces in the alert. Links to the markdown runbook in this repo.

Other fabric alerts:
- `BGPPrefixCountDrop` — prefix count from a peer drops >20% in 5m
- `BFDSessionFlapping` — >3 BFD flaps in 10m
- `EVPNVNIDown` — `frr_evpn_vni_active == 0`
- `RPKIInvalidObserved` — `increase(bmp_rpki_invalid_total[1h]) > 0`

### 7.2 `data_plane_health` (2 alerts) — closes the silent-blackhole gap

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
      Control plane reports healthy (no BGP/BFD alerts) but the active OAM
      probe shows packet loss. Likely silent blackhole.
```

The metric `fabric_probe_loss_ratio` comes from Pushgateway, written by `active_probe.py`. If loss > 50% for 30s, page.

This is the alert that catches the failure where control plane looks 100% green but packets aren't moving. Doesn't exist in most observability setups.

### 7.3 `platform_health` (1 alert) — meta-observability

```yaml
- alert: PrometheusHighSeriesCount
  expr: prometheus_tsdb_head_series > 400000
  for: 10m
  labels:
    severity: warning
    team: observability
  annotations:
    summary: "TSDB series count {{ $value }} approaching 500k limit"
```

The observability stack monitors *itself*. If cardinality drifts toward the hard limit (500k), warn before things fall over.

---

## 8. Walking through the Airflow DAGs

### 8.1 `capacity_report.py`

```python
default_args = {
    'owner': 'network-ops',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

CLICKHOUSE_URL = "http://clickhouse:8123"
ES_URL = "http://elasticsearch:9200"
```

DAG-level defaults. 2 retries with 5-min backoff if a task fails.

```python
def query_prefix_counts(**context):
    query = """
        SELECT toStartOfHour(timestamp) AS hour,
               countIf(withdrawn = 0) AS active_prefixes,
               countIf(withdrawn = 1) AS withdrawn_prefixes
        FROM bgp_routes
        WHERE timestamp >= now() - INTERVAL 24 HOUR
        GROUP BY hour
        ORDER BY hour
        FORMAT JSON
    """
    resp = requests.post(CLICKHOUSE_URL, data=query, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    context['ti'].xcom_push(key='prefix_counts', value=data.get('data', []))
```

Task function. `**context` receives Airflow context (task instance, dag run, etc.). `ti.xcom_push` stores the result for downstream tasks.

`FORMAT JSON` is ClickHouse-specific: returns the result as JSON instead of TSV.

```python
def publish_report(**context):
    ti = context['ti']
    prefix_counts = ti.xcom_pull(key='prefix_counts', task_ids='query_prefix_counts') or []
    top_peers = ti.xcom_pull(key='top_peers', task_ids='query_top_peers') or []
    report = {
        '@timestamp': datetime.utcnow().isoformat(),
        'report_type': 'capacity',
        ...
    }
    requests.post(f"{ES_URL}/capacity-reports/_doc", json=report)
```

Aggregator task. Pulls XCom from both upstream tasks, builds the report, posts to ES.

```python
with DAG('bgp_capacity_report', ...) as dag:
    t_prefix = PythonOperator(task_id='query_prefix_counts', python_callable=query_prefix_counts)
    t_peers = PythonOperator(task_id='query_top_peers', python_callable=query_top_peers)
    t_report = PythonOperator(task_id='publish_report', python_callable=publish_report)
    [t_prefix, t_peers] >> t_report
```

The `>>` operator builds the DAG. `[t_prefix, t_peers] >> t_report` means "report depends on both prefix and peers (in parallel)."

### 8.2 `route_audit.py`

Similar structure, weekly cadence (`schedule_interval='0 8 * * 1'` = Mondays 08:00). Four tasks:
- `detect_churn` — top flappers from ClickHouse
- `detect_new_prefixes` — set difference between this week and last
- `validate_rpki` — query Cloudflare RPKI API for top 20 prefixes
- `publish_audit` — aggregate and POST to ES

---

## 9. SLOs — what we promise

`docs/slos.md` defines SLOs both for the fabric (what we observe) and the pipeline (what we operate).

### Fabric SLOs

- **Fabric-1**: BGP neighbor availability ≥ 99.95% over 30 days (21.6 min budget)
- **Fabric-2**: 99% of link failures detected in ≤ 500ms (BFD-bounded)
- **Fabric-3**: 99% of leaf restarts converge in ≤ 5s (GR `preserve-fw-state`)

### Pipeline SLOs

- **Pipeline-1**: BMP ingest freshness p99 < 5s
- **Pipeline-2**: Prometheus scrape success ≥ 99.9%
- **Pipeline-3**: Airflow DAG SLA — daily capacity_report < 5min, weekly route_audit < 30min
- **Pipeline-4**: Alert delivery success ≥ 99.95%

### Burn-rate alerting

Per Google SRE: multi-window multi-burn-rate. Fast burns (14.4×) page; slow burns (6× over 6h) ticket. Both must hold (AND) to suppress noise.

---

## 10. Cardinality budget

`docs/cardinality-budget.md`. The discipline that prevents Prometheus from falling over.

| Metric | Expected cardinality | Driven by |
|--------|---------------------:|-----------|
| `frr_bgp_peer_state` | ~96 (4 peers × 4 nodes × 6 states) | Peer × node × state |
| `frr_bfd_peer_uptime_seconds` | ~8 | Peer × node |
| `frr_bfd_detect_time_seconds_bucket` | ~80 (histogram) | Same × 10 buckets |
| Total | **~200 series** | Lab scale |

For comparison, a 1000-leaf fabric projects to 50-200k series. Adding `prefix` as a label would multiply by 100k+ — which is why prefix data lives in ClickHouse, not Prometheus.

**Hard limits in Prometheus:**
```yaml
- '--storage.tsdb.head-series-limit=500000'
- '--query.max-samples=50000000'
- '--query.timeout=60s'
```

**Drop rules at scrape time** (example pattern):
```yaml
scrape_configs:
  - job_name: 'frr-fabric'
    metric_relabel_configs:
      - source_labels: [prefix]
        action: drop
        regex: .+
      - regex: '(pid|instance_uuid)'
        action: labeldrop
```

---

## 11. Retention policy

`docs/retention.md`. Each store has its own retention semantics:

| Store | TTL / retention |
|-------|-----------------|
| ClickHouse `bgp_routes` | 90 days delete, 30 days cold transition |
| ClickHouse `bgp_peer_events` | 180 days |
| ClickHouse `rpki_invalid_observations` | 365 days |
| ClickHouse MaterializedViews | 365 days |
| Prometheus local | 15 days |
| Prometheus via Thanos → MinIO | 30d raw, 1y 5m-downsampled, 5y 1h-downsampled |
| Elasticsearch (ILM) | 7d hot / 30d warm / 90d cold / delete |
| Audit logs (deploy-events, controller-events) | 1 year via Vector → S3 (in production) |

---

## 12. Operational walkthrough

### 12.1 Bring up

```bash
$ docker-compose up -d
[+] Running 10/10
 ✔ openbmp        Started
 ✔ clickhouse     Started   # init schema.sql runs on first boot
 ✔ elasticsearch  Started
 ✔ logstash       Started
 ✔ prometheus     Started
 ✔ alertmanager   Started
 ✔ pushgateway    Started
 ✔ thanos         Started
 ✔ minio          Started
 ✔ grafana        Started
 ✔ airflow        Started   # standalone mode; takes ~30s
```

Access points:
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Airflow: http://127.0.0.1:8080
- Kibana: http://localhost:5601
- Alertmanager: http://127.0.0.1:9093

### 12.2 Validate

```bash
$ make validate
# Checks each container in clab-fabric-evpn is reachable from the obs stack
```

### 12.3 Watch a real alert fire

```bash
# In the fabric repo, kill a BGP session
$ docker exec clab-fabric-evpn-leaf1 vtysh -c "clear bgp 10.10.1.1 graceful"

# Watch BGPNeighborDown fire (after 1 minute)
$ curl http://127.0.0.1:9093/api/v2/alerts | jq
```

### 12.4 Verify drift via ClickHouse

```bash
$ docker exec clab-observability-clickhouse-1 clickhouse-client --query="
SELECT prefix, origin_as, count() AS state_changes
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY prefix, origin_as
ORDER BY state_changes DESC
LIMIT 10
"
```

Top 10 flapping prefixes in the last 24h.

---

## 13. HA notes

`docs/ha-notes.md` — every service is single-replica in this lab. Documented failure modes per service, recovery paths, RPO/RTO targets, and what changes for fleet-scale deployment:

| Service | Lab | Fleet scale |
|---------|-----|-------------|
| OpenBMP | Single instance | LB cluster, primary + standby |
| ClickHouse | Single node | 3 replicas, Distributed engine |
| Elasticsearch | Single | 3 master / 3+ data with replicas 1 |
| Prometheus | Single | 2× per region, Thanos query dedup |
| Thanos | Sidecar only | Receive mode for push ingestion |
| MinIO | Single | 4-node erasure-coded |
| Airflow | Standalone | Celery + Redis + Postgres |
| Grafana | Single | 2× stateless behind LB |
| Alertmanager | Single | 3-node cluster for quorum |

---

## 14. Threat model — what could go wrong

(Implicit, not yet a dedicated `THREAT_MODEL.md` — same trust-boundary discipline applies):

- **Public endpoints exposed:** Logstash UDP/514 and OpenBMP TCP/11019 accept any input. In production, restrict via firewall or VPN.
- **No authn on Grafana/Prometheus.** Default `admin/admin`. Lab only.
- **MinIO bound to loopback** (`127.0.0.1:9000`) — Thanos can reach it via Docker DNS but external traffic can't.
- **No TLS internally.** Production would have mTLS via service mesh.

---

## 15. What this repo deliberately doesn't do

- **No PromQL recording rules.** Hot queries should be pre-computed.
- **No DBT layer on ClickHouse.** Direct SQL in DAGs — production would have a semantic layer.
- **No federation across regions.** Single Thanos sidecar; production has `thanos receive` + multi-region querier.
- **No customer-experience SLOs.** Fabric SLOs are infrastructure-level; user-experience SLOs live in higher tiers.
- **No incident management integration.** Alerts route to log webhook by default; production wires PagerDuty / ServiceNow.
- **frr_exporter sidecar deployment.** Scrape configs reference targets that need to actually run frr_exporter — see `clab-fabric-evpn/docs/frr-exporter.md`.

---

## 16. What to read next

1. [`clickhouse/schema.sql`](../clickhouse/schema.sql) — every BMP table + materialized views
2. [`configs/prometheus/alerts.yml`](../configs/prometheus/alerts.yml) — 8 alerts in 3 groups
3. [`runbooks/`](../runbooks/) — one per fabric alert
4. [`docs/slos.md`](slos.md), [`docs/cardinality-budget.md`](cardinality-budget.md), [`docs/retention.md`](retention.md), [`docs/ha-notes.md`](ha-notes.md), [`docs/active-oam.md`](active-oam.md)
5. [`docs/adr/`](adr/) — ClickHouse-vs-TimescaleDB, three-layer, Airflow-vs-cron rationale
6. **Sibling repos:**
   - [`clab-fabric-evpn`](https://github.com/pradeepbc86/clab-fabric-evpn) — the BGP source we observe
   - [`clab-automation`](https://github.com/pradeepbc86/clab-automation) — emits `deploy-events.jsonl` we ingest
   - [`clab-ai-mcp`](https://github.com/pradeepbc86/clab-ai-mcp) — agent that queries our ClickHouse via `query_clickhouse` tool
