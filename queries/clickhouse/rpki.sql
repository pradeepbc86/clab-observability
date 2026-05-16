-- RPKI analytics — ClickHouse SQL

-- Q: RPKI invalid prefixes observed in the last 24h (with frequency)
SELECT
    prefix,
    origin_as,
    count() AS observations,
    groupUniqArray(peer_ip) AS observed_via_peers,
    min(timestamp) AS first_seen,
    max(timestamp) AS last_seen
FROM bgp_routes b
JOIN rpki_invalid_observations r ON b.prefix = r.prefix AND b.origin_as = r.origin_as
WHERE b.timestamp >= now() - INTERVAL 24 HOUR
GROUP BY prefix, origin_as
ORDER BY observations DESC;

-- Q: ROA coverage by AS (what % of our advertisements are RPKI-covered)
SELECT
    origin_as,
    count(DISTINCT prefix) AS total_prefixes,
    countIf(rpki_status = 'valid') AS valid_prefixes,
    countIf(rpki_status = 'unknown') AS unknown_prefixes,
    round(100.0 * countIf(rpki_status = 'valid') / count(), 2) AS coverage_pct
FROM bgp_prefix_analytics
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY origin_as
ORDER BY total_prefixes DESC;
