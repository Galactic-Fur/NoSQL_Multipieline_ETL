SELECT
    substr(datetime, 1, 11) AS log_date,
    hour(from_unixtime(unix_timestamp(datetime,
    'dd/MMM/yyyy:HH:mm:ss Z'))) AS log_hour,

    SUM(CASE WHEN status >= 400 AND status <= 599
        THEN 1 ELSE 0 END) AS error_request_count,

    COUNT(*) AS total_request_count

FROM nasa_logs

GROUP BY
    substr(datetime, 1, 11),
    hour(from_unixtime(unix_timestamp(datetime,
    'dd/MMM/yyyy:HH:mm:ss Z')));