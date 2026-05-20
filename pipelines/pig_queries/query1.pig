logs = LOAD '$INPUT'
USING TextLoader()
AS (line:chararray);

parsed = FOREACH logs GENERATE
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line,
            '(\\S+) - - \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)'
        )
    );

grp = GROUP parsed BY $1;

summary = FOREACH grp GENERATE
    group,
    COUNT(parsed);

DUMP summary;