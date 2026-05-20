logs = LOAD '$INPUT'
USING TextLoader()
AS (line:chararray);

errors = FILTER logs BY
    line MATCHES '.* (4..|5..) .*';

grp = GROUP errors ALL;

summary = FOREACH grp GENERATE
    COUNT(errors);

DUMP summary;