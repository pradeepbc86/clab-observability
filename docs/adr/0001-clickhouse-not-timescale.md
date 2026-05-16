# ADR 0001 — ClickHouse for BGP route analytics, not TimescaleDB / InfluxDB

**Status:** Accepted
**Date:** 2026-05-16

## Context

BGP routing data has a peculiar shape: each route is a row, prefixes appear/disappear constantly, and queries are usually "give me all rows matching prefix X" or "aggregate by AS over a window". This is wide-column / time-series-ish but not metric-shaped.

## Decision

ClickHouse with `MergeTree` engine, `ORDER BY (timestamp, prefix, peer_asn)`.

## Rationale

| Dimension | ClickHouse | TimescaleDB | InfluxDB | Prometheus |
|-----------|-----------|-------------|----------|------------|
| Cardinality tolerance | High (millions of distinct prefixes) | Medium | Low (label-cardinality issues like Prom) | Low |
| Columnar compression on prefix strings | Excellent (LZ4 ~10x) | Good | Limited | n/a |
| Aggregation speed on AS-path arrays | Native `Array(UInt32)` + array fns | Manual unnest via SQL | Limited | n/a |
| Joins across tables | First-class | First-class (Postgres) | Limited | Federated only |
| Operator familiarity | Specialized tooling | SQL/Postgres ecosystem | Native client tools | PromQL |
| Disk footprint at 1M routes/h | ~ few GB/month with compression | ~10x larger | Comparable to ClickHouse | n/a |

BMP feeds easily push 100k-1M update messages per hour at edge scale. ClickHouse digests this without flinching; PromQL-shaped TSDBs (Prom/Influx) blow up on the `prefix` label dimension.

## Trade-offs accepted

- Operators need to learn ClickHouse SQL. We mitigate by maintaining `queries/clickhouse/*.sql` as a working-example library.
- No native histogram math like PromQL. We use Prometheus *alongside* ClickHouse, not instead of — Prom holds the metric-shaped data (`frr_bgp_peer_state`, `frr_bfd_detect_time`), ClickHouse holds the wide route table.
- ClickHouse replication setup is non-trivial. Lab uses a single-node `clickhouse-server` container.

## When we'd revisit

- If data volume drops below ~1k updates/min, TimescaleDB becomes attractive (familiar SQL, single tool covers metrics + analytics).
- If we need ACID transactions on route data (we don't), Postgres / TimescaleDB wins.

## See also

- [OpenBMP-Collector + ClickHouse reference](https://github.com/OpenBMP/openbmp)
- [ClickHouse for observability talk (Altinity 2023)](https://altinity.com/blog/clickhouse-observability)
