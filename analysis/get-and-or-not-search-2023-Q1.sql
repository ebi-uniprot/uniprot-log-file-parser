SET memory_limit='32GB';
SET threads TO 8;
.open --readonly /hps/nobackup/martin/uniprot/users/dlrice/rest-api.duckdb
COPY (
    SELECT
        request
    FROM
        uniprotkb AS uni
    JOIN
        useragent AS ua ON uni.useragent_id = ua.id
    JOIN
        useragent_family AS uaf ON uaf.id = ua.family_id
    WHERE
        uaf.type = 'browser' AND
        uaf.major = true AND
        method = 'GET' AND
        status = 200 AND
        datetime BETWEEN '2023-01-01' AND '2023-03-31' AND
        request ILIKE '%/search\?%query=%' ESCAPE '\' AND
        (
            request ILIKE '%20AND%20%' OR
            request ILIKE '%20OR%20%' OR
            request ILIKE '%20NOT%20%' OR
            request ILIKE ' AND ' OR
            request ILIKE ' OR ' OR
            request ILIKE ' NOT '
        ) AND
        request NOT ILIKE '%size=%'
) TO '/hps/nobackup/martin/uniprot/users/dlrice/get-and-or-not-search-2023-Q1.csv' (HEADER, DELIMITER ',');
