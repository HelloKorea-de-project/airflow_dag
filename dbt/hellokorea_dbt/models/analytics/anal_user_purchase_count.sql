SELECT 
	fap.user_id, 
	COUNT(fap.user_id) AS purchase_count
FROM 
	{{ ref('fact_airline_purchase_log') }} fap
JOIN 
	{{ ref('fact_user_session_info') }} fusi ON fap.user_id = fusi.user_id
WHERE 
	fap.user_id IS NOT NULL
GROUP BY 1