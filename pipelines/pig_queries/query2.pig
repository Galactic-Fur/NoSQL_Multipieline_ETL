logs = LOAD '$INPUT'
USING TextLoader()
AS (line:chararray);

grp = GROUP logs BY line;

cnt = FOREACH grp GENERATE
    group,
    COUNT(logs);

ordered = ORDER cnt BY $1 DESC;

top20 = LIMIT ordered 20;

DUMP top20;