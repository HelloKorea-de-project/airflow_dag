SELECT suc.channel, COUNT(1) as search_count
    FROM (SELECT succ.session_id, succ.user_id, sct.channel
					FROM {{ ref('user_click_count') }} succ
					JOIN {{ ref('fact_session_channel_timestamp') }} sct
					ON succ.session_id = sct.session_id) suc
    JOIN {{ ref('fact_airline_search_log') }} apl
		ON suc.user_id = apl.user_id
    GROUP BY 1;