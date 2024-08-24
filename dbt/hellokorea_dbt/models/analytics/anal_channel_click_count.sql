SELECT 
	sct.channel, 
	SUM(ucc.click_count) as click_count
FROM 
	{{ ref('anal_user_click_count') }} ucc
JOIN 
	{{ ref('fact_session_channel_timestamp') }} sct ON ucc.session_id = sct.session_id
GROUP BY 1