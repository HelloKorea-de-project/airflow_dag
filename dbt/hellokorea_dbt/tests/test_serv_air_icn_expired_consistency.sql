WITH consistency_check AS (
	SELECT 
		extracteddate, 
		COUNT(DISTINCT isexpired) AS unique_isexpired_count
		FROM {{ ref('fresh_serv_air_icn') }}
		GROUP BY extracteddate
)
SELECT COUNT(*)
FROM consistency_check
WHERE unique_isexpired_count > 1
