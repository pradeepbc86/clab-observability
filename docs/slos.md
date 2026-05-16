# Service Level Objectives

SLO targets for the observability pipeline and the fabric it watches. Each SLO has a Service Level Indicator (SLI) and an error budget. Burn-rate alerts (multi-window, multi-burn-rate per Google SRE) live in `configs/prometheus/alerts.yml`.

## Fabric SLOs (the network we observe)

### Fabric-1: BGP neighbor availability

- **SLI**: fraction of fabric BGP peer-pairs in `Established` state
- **SLO**: 99.95% over 30 days
- **Error budget**: 21.6 minutes per 30 days
- **Measurement**: `avg_over_time(frr_bgp_peer_state{state="Established"}[30d]) >= 0.9995`
- **Alert at 14.4× burn rate** → page; **at 6× burn rate over 6h** → ticket (Google SRE multi-window)

### Fabric-2: Failure detection latency (BFD)

- **SLI**: time from physical link-down to BFD `Down` state on peer router
- **SLO**: 99% of failures detected in ≤ 500ms over 30 days
- **Error budget**: 1% of failures may exceed 500ms
- **Measurement**: `histogram_quantile(0.99, sum(rate(frr_bfd_detect_time_seconds_bucket[30d])) by (le)) <= 0.5`
- **Target rationale**: BFD configured at `150ms × 3 = 450ms`; 50ms headroom for control-plane scheduling

### Fabric-3: EVPN convergence after leaf restart

- **SLI**: time from `bgpd` restart to last EVPN type-2 route reinstalled remotely
- **SLO**: 99% of restarts converge in ≤ 5 seconds (GR preserve-fw-state means zero data-plane loss)
- **Error budget**: 1% of restarts may take longer
- **Target rationale**: 4-node fabric → bounded route count → fast EVPN reconvergence

## Pipeline SLOs (the observability stack itself)

### Pipeline-1: BMP ingest freshness

- **SLI**: lag between BMP event timestamp and ClickHouse insert timestamp
- **SLO**: p99 < 5 seconds over 7 days
- **Error budget**: 1% of inserts may have >5s lag
- **Measurement**: ClickHouse: `quantile(0.99)(now() - timestamp)` filtered to "recently inserted" rows
- **Why it matters**: stale BMP data → wrong dashboards → wrong decisions

### Pipeline-2: Prometheus scrape success

- **SLI**: scrape success ratio across all targets
- **SLO**: 99.9% over 7 days
- **Error budget**: 0.1% scrapes may fail
- **Measurement**: `avg_over_time(up[7d]) >= 0.999`
- **Alert at 14.4× burn rate** → page

### Pipeline-3: Airflow DAG SLA

- **SLI**: scheduled DAG runs that complete within SLA window
- **SLO**: 99% of `bgp_capacity_report` (daily) finish in ≤ 5 min
- **SLO**: 99% of `bgp_route_audit` (weekly) finish in ≤ 30 min
- **Measurement**: Airflow's built-in `dag_run_duration_seconds`
- **Error budget**: 1% may miss SLA — typically because ClickHouse is overloaded; runbook routes to scaling decision

### Pipeline-4: Alert delivery

- **SLI**: alerts fired to Alertmanager that reach a receiver within 1 minute
- **SLO**: 99.95% over 30 days
- **Why it matters**: a critical alert that doesn't page is worse than no alert at all

## Burn-rate alerting

Per Google SRE *The Site Reliability Workbook* Ch. 5, we use multi-window multi-burn-rate alerts so we page early on fast burns and tolerate slow burns. Pseudo-PromQL for Fabric-1:

```promql
# 1h fast burn (14.4×): page within 5 minutes of a major outage
(
  1 - avg_over_time(frr_bgp_peer_state{state="Established"}[1h])
) > (14.4 * (1 - 0.9995))

# 6h slow burn (6×): ticket within ~30 minutes of gradual degradation
(
  1 - avg_over_time(frr_bgp_peer_state{state="Established"}[6h])
) > (6 * (1 - 0.9995))
```

Both must fire (AND) to suppress single-window noise.

## Cardinality budget

See `docs/cardinality-budget.md`. SLO health depends on Prometheus not blowing up.

## What this lab doesn't model

- **Multi-region SLO aggregation** — Thanos query layer would federate per-region Prometheus instances. Single-DC here.
- **Customer-facing SLOs** — fabric SLOs above are infrastructure SLIs, not the customer-experience SLOs (web app availability etc.) that would sit on top.
- **Error budget policies** — what we *do* when budget is burned (freeze deploys? page exec?). Document in your org's playbook.

## References

- [The Site Reliability Workbook (Google)](https://sre.google/workbook/alerting-on-slos/)
- [Cloudflare on SLOs](https://blog.cloudflare.com/measuring-the-internets-availability/)
