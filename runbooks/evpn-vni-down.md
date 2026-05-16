# Runbook: EVPNVNIDown

**Alert:** `frr_evpn_vni_active == 0` — a VNI stopped advertising Type-2/3 routes.
**Severity:** critical
**Owner:** network-ops

## Customer impact

Hosts behind this leaf in the affected VNI lose L2 reachability across the fabric. Distributed anycast gateway still answers ARP locally (so the host's default gateway works), but cross-leaf traffic is black-holed until VNI recovers.

## Diagnosis

1. **VNI state on the affected leaf:**
   ```bash
   docker exec clab-obs-telemetry-<instance> vtysh -c "show evpn vni <vni>"
   ```
   Expected: `VxLAN Interface: vxlan<vni>`, `Local VTEP IP`, `Remote VTEPs: N`.

2. **Underlying bridge / vxlan kernel state:**
   ```bash
   docker exec clab-obs-telemetry-<instance> ip -d link show vxlan<vni>
   docker exec clab-obs-telemetry-<instance> bridge fdb show dev vxlan<vni>
   ```

3. **EVPN BGP routes — is the leaf advertising/receiving Type-3?**
   ```bash
   docker exec clab-obs-telemetry-<instance> vtysh -c "show bgp l2vpn evpn route type 3"
   ```

4. **`advertise-all-vni` config check:**
   ```bash
   docker exec clab-obs-telemetry-<instance> vtysh -c "show running-config bgpd" | grep -A20 'l2vpn evpn'
   ```

## Common causes

| Cause | Fix |
|-------|-----|
| `vxlan<vni>` interface disappeared (init script failed) | Re-run `configs/leaf*/setup.sh` |
| `bgpd` lost its EVPN session to spines | Cross-reference `BGPNeighborDown` runbook |
| FIB programming failed (kernel netlink errors in zebra log) | `docker logs clab-... | grep zebra` |
| Underlay route to remote VTEP withdrawn | `vtysh -c "show ip route 10.0.0.4"` (other leaf's VTEP) |

## Mitigation

Bounce the VNI gracefully:
```bash
docker exec clab-obs-telemetry-<instance> vtysh -c "clear bgp l2vpn evpn *"
```

If `vxlan<vni>` interface is missing, re-apply leaf setup:
```bash
docker exec clab-obs-telemetry-<instance> bash /etc/frr/setup.sh
```
