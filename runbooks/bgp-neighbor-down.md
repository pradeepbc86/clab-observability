# Runbook: BGPNeighborDown

**Alert:** `BGPNeighborDown` — BGP peer not Established for ≥ 1 minute.
**Severity:** critical
**Owner:** network-ops

## Symptoms

- Prometheus alert fired with `peer` and `instance` labels
- Kibana shows `%BGP-5-ADJCHANGE: ... is Idle/Active/Connect` syslog events
- Grafana "BGP Routes" dashboard shows prefix count from peer dropped to 0

## Diagnosis

1. **Identify the peer + local interface:**
   ```bash
   docker exec clab-observability-<instance> vtysh -c "show bgp neighbor <peer>"
   ```

2. **Check BFD state** (fast indicator of link layer):
   ```bash
   docker exec clab-observability-<instance> vtysh -c "show bfd peer <peer>"
   ```
   - `state: Down` with `diagnostic: Echo Function Failed` → physical/MTU
   - `state: Down` with `diagnostic: Control Detection Time Expired` → upstream loss
   - No BFD session → BGP misconfig, BFD never came up

3. **Check syslog history in Kibana:**
   - Index `bgp-events-*`, filter `neighbor_ip:"<peer>"`
   - Look for repeated flap pattern → physical
   - Single drop → check peer-side config / MD5 / TCP-AO

4. **Check FIB:**
   ```bash
   docker exec clab-observability-<instance> ip route show proto bgp
   ```

## Mitigation

| Cause | Action |
|-------|--------|
| Physical link down | Replace cable/optic. Confirm `ip link show <iface>` is UP. |
| MTU mismatch | Set matching MTU both sides; BFD echo fails on first-fragment-needed. |
| Hold-timer too aggressive after a peer restart | Temporarily bump hold to 30s; restore once stable. |
| Peer-side config rolled back | Coordinate with peer team; review change ticket. |
| BGP TCP MD5/TCP-AO mismatch | Re-derive shared secret; restart peer session. |

## Recovery validation

```bash
# Both sides Established
docker exec clab-observability-<instance> vtysh -c "show bgp summary"
# Prefix counts back to baseline (compare to Grafana 24h prior)
# BFD session Up
docker exec clab-observability-<instance> vtysh -c "show bfd peers"
```

Alert should auto-resolve within `for: 1m` after Established.

## Postmortem

If neighbor was down > 5 minutes, file a postmortem doc. Cross-reference with `bgp_route_audit` weekly DAG to confirm no prefix exfiltration during the window.
