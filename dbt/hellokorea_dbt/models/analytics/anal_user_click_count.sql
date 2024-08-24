SELECT 
	user_id, 
	session_id, 
	COUNT(user_id) AS click_count
FROM
	{{ ref('fact_airline_click_log') }}
WHERE user_id IS NOT NULL
GROUP BY 1,2