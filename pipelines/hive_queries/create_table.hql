CREATE EXTERNAL TABLE IF NOT EXISTS nasa_logs (
    host STRING,
    identity STRING,
    user STRING,
    datetime STRING,
    request STRING,
    status INT,
    bytes BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) - - \\[(.*?)\\] \"(.*?)\" ([0-9]{3}) ([0-9-]*)"
)
STORED AS TEXTFILE
LOCATION '/input/nasa_logs';