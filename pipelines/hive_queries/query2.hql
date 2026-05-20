SELECT
    request,
    COUNT(*) AS request_count,
    SUM(bytes) AS total_bytes
FROM nasa_logs
GROUP BY request
ORDER BY request_count DESC
LIMIT 20;