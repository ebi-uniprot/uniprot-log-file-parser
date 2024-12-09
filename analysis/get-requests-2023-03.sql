SET memory_limit='32GB';
SET threads TO 8;
.open --readonly /hps/nobackup/martin/uniprot/users/dlrice/rest-api.duckdb
COPY (
	SELECT
		COUNT(*) AS requests
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
) TO '/hps/nobackup/martin/uniprot/users/dlrice/get-requests-march-23.csv' (HEADER, DELIMITER ',');
