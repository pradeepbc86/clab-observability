# ADR 0002 — Three storage tiers: ClickHouse, Elasticsearch, Prometheus

**Status:** Accepted
**Date:** 2026-05-16

## Context

Network telemetry has at least three distinct shapes:

1. **Wide, high-cardinality routing data** (BGP routes via BMP — prefix, AS-path, communities)
2. **Free-text structured events** (syslog — neighbor flap messages, config changes)
3. **Numeric metrics with fixed labels** (frr_exporter — neighbor state count, BFD detect time)

A single storage backend handles one of these well and the other two badly. A common mistake is forcing everything into Prometheus (cardinality explosion) or into Elasticsearch (slow numeric aggregation).

## Decision

Three backends, one per shape:

| Shape | Backend | Why |
|-------|---------|-----|
| Wide routing data | ClickHouse + OpenBMP collector | See ADR 0001 — handles prefix-cardinality |
| Free-text events | Elasticsearch + Logstash | Mature DSL queries, Kibana timelines |
| Metrics | Prometheus + Thanos for long-term, MinIO as the object store | Standard for SRE, integrates with everything |

Grafana sits on top of all three as the unified viewing layer.

## Rationale

Unified storage tools (e.g., Splunk, Datadog, ELK-as-everything) work but at significantly higher cost or with significant operational limits. At fabric scale you very rapidly hit one of:

- Cardinality limits (Prom, Influx)
- Numeric-aggregation cost (Elasticsearch)
- License cost (Splunk, Datadog)

Specializing each tier accepts the operational overhead of running three backends in exchange for each one solving its problem efficiently.

## Trade-offs accepted

- Three datastores to operate, alert on, capacity-plan.
- Cross-tier queries require a join in the dashboard / Airflow DAG layer rather than at the storage layer. We accept this — the `bgp_capacity_report` and `bgp_route_audit` DAGs in `airflow/dags/` show how.
- Onboarding ramp: operators need to learn ClickHouse SQL + Lucene/DSL + PromQL. Mitigated by `queries/` gallery.

## When we'd consolidate

- If route volume drops to "hundreds of updates per hour", merge route analytics into Elasticsearch.
- If we adopt VictoriaMetrics (which now supports both metrics and logs at cardinality), VM could replace Prom + ES for a smaller fabric.

## See also

- [Thanos architecture](https://thanos.io/v0.32/thanos/quick-tutorial.md/)
- [OpenBMP-Collector design notes](https://github.com/OpenBMP/openbmp/wiki)
