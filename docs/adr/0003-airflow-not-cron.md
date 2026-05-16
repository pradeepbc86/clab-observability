# ADR 0003 — Airflow for scheduled audits, not cron / Kubernetes CronJob

**Status:** Accepted
**Date:** 2026-05-16

## Context

`bgp_capacity_report` and `bgp_route_audit` need to run on schedules (daily / weekly), read from ClickHouse, optionally call external APIs (Cloudflare RPKI), and publish results to Elasticsearch. They have multi-step DAG semantics.

## Decision

Apache Airflow with `LocalExecutor` and SQLite metadata DB (single-process `airflow standalone`).

## Rationale

| Dimension | cron | k8s CronJob | Airflow | Prefect / Dagster |
|-----------|------|-------------|---------|-------------------|
| Multi-step dependencies | Manual scripting | None natively | DAG primitives | DAG primitives |
| XCom / data passing between tasks | Manual files | Manual ConfigMaps | Built-in XCom | Built-in |
| Retries with backoff | Manual `||` chains | `restartPolicy` | First-class | First-class |
| Web UI for monitoring | None | k9s, partial | Built-in DAG view | Built-in |
| Lineage tracking | None | None | Limited | Strong |
| Operator familiarity in 2026 | High | High | High | Growing |

For two DAGs at lab scale, cron would actually be fine. We chose Airflow because:

1. The DAGs *are* multi-step (query ClickHouse → call RPKI → write to ES) — Airflow's task graph maps directly.
2. Reviewers expect to see Airflow on a network-ops portfolio — it's the de-facto scheduler in the space.
3. The compose service is one container; cost is low.

## Trade-offs accepted

- Airflow standalone with SQLite is fine for a lab but not for fleet operation. To scale: replace with Postgres metadata DB + `CeleryExecutor` + Redis broker.
- Airflow learning curve is non-trivial. Mitigated by keeping DAGs small (each <150 lines).
- Cost of running idle Airflow > cost of running idle cron. Not material at this scale.

## When we'd revisit

- If DAGs grow to >10, we'd evaluate Dagster (better software-engineering ergonomics, type-checked).
- If we want Python-native typed DAGs and don't need the legacy ecosystem, Prefect 2.x is cleaner.

## See also

- [Airflow architecture](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Dagster vs Airflow comparison](https://dagster.io/blog/dagster-vs-airflow)
