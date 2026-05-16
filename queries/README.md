# Query gallery

Reference PromQL and ClickHouse SQL queries for the most common operational questions. Each query is annotated with the question it answers, so you can grep by intent.

## Files

| File | Source | Topic |
|------|--------|-------|
| `prometheus/bgp_state.promql` | Prometheus | BGP neighbor state, BFD, convergence times |
| `prometheus/cardinality.promql` | Prometheus | Cardinality budget audits — catch series explosions |
| `clickhouse/bgp_analytics.sql` | ClickHouse | Route churn, top peers, prefix history, anycast verification |
| `clickhouse/rpki.sql` | ClickHouse | RPKI invalid history, ROA coverage |
| `elasticsearch/bgp_events.txt` | Elasticsearch DSL | Neighbor flap timeline, syslog correlation |
