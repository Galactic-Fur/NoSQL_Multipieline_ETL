SELECT
    substr(datetime, 1, 11) AS log_date,
    status,
    COUNT(*) AS request_count,
    SUM(bytes) AS total_bytes
FROM nasa_logs
GROUP BY substr(datetime, 1, 11), status;