SET memory_limit='14GB';
SET threads TO 4;
.open --readonly /hps/nobackup/martin/uniprot/users/dlrice/rest-api.duckdb
COPY (
    SELECT
        bytes,
        request,
        uaf.family,
        uaf.type
    FROM
        uniprotkb AS uni
    JOIN
        useragent AS ua ON uni.useragent_id = ua.id
    JOIN
        useragent_family AS uaf ON uaf.id = ua.family_id
    WHERE
        uaf.major = true AND
        method = 'GET' AND
        status = 200 AND
        datetime BETWEEN '2023-03-01' AND '2023-03-31' AND
        request ILIKE '%/stream\?%' ESCAPE '\'
) TO '/hps/nobackup/martin/uniprot/users/dlrice/get-stream-format-2023-03.csv' (HEADER, DELIMITER ',');
