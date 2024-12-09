SET memory_limit='32GB';
SET threads TO 8;
.open --readonly /hps/nobackup/martin/uniprot/users/dlrice/rest-api.duckdb
COPY (
	SELECT
		COUNT(uaf.family) 
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
            datetime BETWEEN '2023-03-01' AND '2023-03-31'
	GROUP BY
	    uaf.family
        ORDER BY
            uaf.family	
) TO '/hps/nobackup/martin/uniprot/users/dlrice/get-requests-useragent-family-2023-03.csv' (HEADER, DELIMITER ',');
