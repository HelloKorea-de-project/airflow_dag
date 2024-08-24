SELECT 
	fusi.channel, 
	COUNT(1) AS purchase_count
FROM {{ ref('fact_user_session_info') }} fusi
JOIN {{ ref('fact_airline_purchase_log') }} apl ON fusi.user_id = apl.user_id
GROUP BY 1