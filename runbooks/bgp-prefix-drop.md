# Runbook: BGPPrefixCountDrop

**Alert:** Received-prefix count from a peer dropped > 20% within 5 minutes.
**Severity:** warning
**Owner:** network-ops

## Why this matters

A sudden prefix-count drop without a session reset usually means the peer applied a more restrictive outbound filter (or one of *our* inbound filters started denying more). Either way, traffic for the dropped destinations is now being routed elsewhere or blackholed.

## Diagnosis

1. **Compare current vs prior advertised set** (ClickHouse):
   ```sql
   SELECT
     prefix, origin_as,
     argMax(action, timestamp) AS last_action,
     max(timestamp) AS last_seen
   FROM bgp_routes
   WHERE peer_ip = '{{ peer }}'
     AND timestamp >= now() - INTERVAL 1 HOUR
   GROUP BY prefix, origin_as
   HAVING last_action = 'withdraw'
   ORDER BY last_seen DESC
   LIMIT 100
   ```

2. **Categorize the withdrawn set:**
   - Same origin AS, contiguous prefixes → upstream aggregation event
   - Mixed origin AS, scattered → policy change on peer side
   - Single /24 → likely customer disconnect

3. **Check our inbound route-map hits:**
   ```bash
   docker exec clab-observability-spine1 vtysh -c "show route-map RM-LEAVES-IN"
   ```

## Mitigation

Usually no action needed if peer-initiated. If our inbound filter is at fault:
1. Revert the route-map change via Git
2. Apply via `clab-automation` deploy pipeline
3. Watch prefix counts recover within ~30s

## Auto-resolution

Alert clears once prefix count returns within 20% of the 5-minute prior baseline.
