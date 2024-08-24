SELECT 
	ucc.session_id, 
	ucc.user_id, 
	sct.channel
FROM 
	{{ ref('fact_airline_click_log') }} ucc
JOIN 
	{{ ref('fact_session_channel_timestamp') }} sct ON ucc.session_id = sct.session_id
GROUP BY 1,2,3