SELECT 
	suc.channel, 
	COUNT(1) as search_count
FROM 
	{{ ref('fact_session_channel_timestamp') }} suc
JOIN 
	{{ ref('fact_airline_search_log') }} apl
ON suc.session_id = apl.session_id
GROUP BY 1