-- BGP Routes table for BMP monitoring
CREATE TABLE IF NOT EXISTS default.bgp_routes (
    timestamp DateTime,
    prefix String,
    origin_as UInt32,
    peer_ip String,
    peer_asn UInt32,
    action String,
    path Array(UInt32),
    communities Array(String),
    withdrawn UInt8,
    monitor_ip String
) ENGINE = MergeTree()
ORDER BY (timestamp, prefix, peer_asn);

-- BGP Route summary (aggregated by hour)
CREATE TABLE IF NOT EXISTS default.bgp_routes_hourly (
    timestamp DateTime,
    prefix_count UInt64,
    withdrawn_count UInt64,
    top_peer_asn UInt32,
    churn_rate Float32
) ENGINE = MergeTree()
ORDER BY (timestamp);

-- BGP Neighbor state tracking
CREATE TABLE IF NOT EXISTS default.bgp_neighbors (
    timestamp DateTime,
    neighbor_ip String,
    neighbor_asn UInt32,
    state String,
    state_change_count UInt32,
    routes_advertised UInt32,
    routes_received UInt32,
    uptime_seconds UInt64
) ENGINE = MergeTree()
ORDER BY (timestamp, neighbor_ip, neighbor_asn);

-- BGP Prefix analytics
CREATE TABLE IF NOT EXISTS default.bgp_prefix_analytics (
    timestamp DateTime,
    prefix String,
    origin_as UInt32,
    churn_count UInt32,
    last_seen DateTime,
    first_seen DateTime,
    total_path_changes UInt32
) ENGINE = MergeTree()
ORDER BY (timestamp, prefix);
