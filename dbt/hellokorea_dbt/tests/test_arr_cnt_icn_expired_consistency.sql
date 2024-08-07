WITH consistency_check AS (
	SELECT 
		extracteddate, 
		COUNT(DISTINCT isexpired) AS unique_isexpired_count
		FROM {{ ref('arr_cnt_icn') }}
		GROUP BY extracteddate
)
SELECT COUNT(*)
FROM consistency_check
WHERE unique_isexpired_count > 1
