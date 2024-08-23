SELECT succ.session_id, succ.user_id, sct.channel, SUM(succ.click_cnt)
FROM (( ref('user_click_count') }} succ
JOIN {{ ref('fact_session_channel_timestamp') }} sct
ON succ.session_id = sct.session_id) suc