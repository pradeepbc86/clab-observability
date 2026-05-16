-- BGP route analytics — ClickHouse SQL gallery

-- Q: Top 10 flapping prefixes in the last 24h
SELECT
    prefix,
    origin_as,
    count() AS state_changes,
    min(timestamp) AS first_change,
    max(timestamp) AS last_change
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY prefix, origin_as
ORDER BY state_changes DESC
LIMIT 10;

-- Q: Per-hour prefix count over last 7 days (capacity trending)
SELECT
    toStartOfHour(timestamp) AS hour,
    count(DISTINCT prefix) AS active_prefixes
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 7 DAY
  AND withdrawn = 0
GROUP BY hour
ORDER BY hour;

-- Q: Top 20 peers by route count (right now)
SELECT
    peer_ip,
    peer_asn,
    count(DISTINCT prefix) AS prefix_count,
    max(timestamp) AS last_seen
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 5 MINUTE
  AND withdrawn = 0
GROUP BY peer_ip, peer_asn
ORDER BY prefix_count DESC
LIMIT 20;

-- Q: Anycast verification — same prefix from multiple monitors?
-- Useful for catching split-brain where one POP sees a different route
SELECT
    prefix,
    origin_as,
    groupUniqArray(monitor_ip) AS seen_by_monitors,
    length(groupUniqArray(monitor_ip)) AS monitor_count
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 1 HOUR
  AND withdrawn = 0
GROUP BY prefix, origin_as
HAVING monitor_count < (SELECT count(DISTINCT monitor_ip) FROM bgp_routes WHERE timestamp >= now() - INTERVAL 1 HOUR)
ORDER BY monitor_count ASC
LIMIT 50;

-- Q: New prefixes seen this week vs last week (delta analysis)
SELECT DISTINCT prefix, origin_as
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 7 DAY
  AND withdrawn = 0
  AND prefix NOT IN (
    SELECT DISTINCT prefix FROM bgp_routes
    WHERE timestamp BETWEEN now() - INTERVAL 14 DAY AND now() - INTERVAL 7 DAY
      AND withdrawn = 0
  );

-- Q: Reconstruct fabric state at a specific timestamp (incident replay)
SELECT
    prefix,
    origin_as,
    peer_ip,
    argMax(action, timestamp) AS state_at_t,
    max(timestamp) AS last_change_before_t
FROM bgp_routes
WHERE timestamp <= '2026-05-16 14:23:00'
GROUP BY prefix, origin_as, peer_ip
HAVING state_at_t = 'announce';

-- Q: AS-path length distribution — long paths indicate suboptimal routing
SELECT
    length(path) AS as_path_length,
    count() AS prefix_count
FROM bgp_routes
WHERE timestamp >= now() - INTERVAL 1 HOUR
  AND withdrawn = 0
GROUP BY as_path_length
ORDER BY as_path_length;
