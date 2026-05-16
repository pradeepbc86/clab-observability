# Runbook: RPKIInvalidObserved

**Alert:** ≥1 RPKI-invalid prefix observed in BMP feed within the last hour.
**Severity:** warning
**Owner:** network-ops

## Why this matters

An RPKI-invalid prefix means a router somewhere in the fabric (or an upstream we're observing via BMP) accepted a route whose origin AS doesn't match what's in the RPKI repository. Could be:

- Misconfigured legitimate origin
- Genuine prefix hijack
- Stale RPKI cache on the validator
- Our BMP unmarshaler bug (rare but check first if alert is bursty)

## Diagnosis

1. **Pull the invalid prefix list from ClickHouse:**
   ```sql
   SELECT prefix, origin_as, peer_ip, monitor_ip,
          min(timestamp) AS first_seen,
          max(timestamp) AS last_seen,
          count() AS observations
   FROM bgp_routes
   WHERE timestamp >= now() - INTERVAL 1 HOUR
     AND withdrawn = 0
     AND prefix IN (
       -- prefixes that route_audit DAG flagged invalid this run
       SELECT prefix FROM rpki_invalid_observations
       WHERE timestamp >= now() - INTERVAL 1 HOUR
     )
   GROUP BY prefix, origin_as, peer_ip, monitor_ip
   ORDER BY last_seen DESC
   ```

2. **Cross-check live against an external validator:**
   ```bash
   curl -s "https://rpki.cloudflare.com/api/v1/validity?asn=AS<origin>&prefix=<prefix>" | jq
   ```
   If Cloudflare says `valid` but we logged `invalid`, suspect stale local cache.

3. **Check the BMP-receiving router's view:**
   ```bash
   docker exec clab-observability-spine1 vtysh -c "show bgp ipv4 <prefix>"
   ```

## Mitigation

| Finding | Action |
|---------|--------|
| Legitimate but misregistered prefix (we own it) | File ROA update via RIR; alert clears once RPKI cache refreshes (~1h) |
| Genuine hijack from external AS | Apply blackhole community to the prefix at edge; escalate to peering team |
| Stale validator cache | Force refresh: `rpki-rtr-mgr -r` or equivalent; cross-reference with route-server |
| Unmarshal bug | File issue; suppress alert until root-caused |

## Avoid

- Do not auto-blackhole based on this alert alone. RPKI invalid does **not** equal hijack. Always involve a human.
