SELECT 
	user_id, 
	session_id, 
	COUNT(user_id) AS search_count
FROM 
	{{ ref('fact_airline_search_log') }}
WHERE 
	user_id IS NOT NULL
GROUP BY 
	1,2