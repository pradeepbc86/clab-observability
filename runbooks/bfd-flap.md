# Runbook: BFDSessionFlapping

**Alert:** BFD session bounced > 3 times in 10 minutes.
**Severity:** warning
**Owner:** network-ops

## Why this matters

BFD is the fabric's sub-second failure detector. Flapping BFD doesn't always tear down BGP (depending on multiplier), but it does cause SPF/ECMP re-computation and FIB churn — measurable in `frr_bfd_session_state_changes_total`.

Common root causes, ordered by frequency:

| Cause | Signal |
|-------|--------|
| Optic / cable | `dmesg` shows link-down/up cycles |
| MTU mismatch on aggressive BFD echo | BFD diag = "Echo Function Failed" |
| CoPP / control-plane policer dropping BFD | High CPU on peer; BFD packets arrive but late |
| Asymmetric routing causing BFD reverse-path loss | uRPF logs |
| Peer reload loop | Peer's uptime keeps resetting |

## Diagnosis

1. **Identify the session:**
   ```bash
   docker exec clab-observability-<instance> vtysh -c "show bfd peers"
   ```

2. **History of state changes:**
   ```promql
   changes(frr_bfd_peer_uptime_seconds{peer="<peer>"}[1h])
   ```

3. **Correlate with physical:**
   ```bash
   docker exec clab-observability-<instance> ip link show <iface>
   docker exec clab-observability-<instance> ethtool -S <iface> | grep -i error
   ```

4. **Check CoPP / control-plane drops** (if applicable on vendor):
   ```bash
   # vendor-specific — on FRR via zebra:
   docker exec clab-observability-<instance> vtysh -c "show zebra"
   ```

## Mitigation

- If optic/cable suspected: swap, retest.
- If MTU: align both sides at link MTU (typical fabric is 9216).
- If aggressive timers (50ms × 3 = 150ms detect): relax to `300ms × 3` on transit links until physical is stable.
- If peer reload loop: page peer team; this is their issue.

## Avoid

- Do **not** disable BFD as a workaround. Loss of sub-second detection is worse than the flap noise. Raise hold-down via dampening instead.
