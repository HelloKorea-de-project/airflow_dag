SELECT 
	u.country, 
	SUM(usc.search_count) AS search_count, 
	SUM(ucc.click_count) AS click_count, 
	SUM(upc.purchase_count) AS purchase_count
FROM 
	{{ ref('dim_user_info') }} u 
JOIN 
	{{ ref('anal_user_search_count') }} usc ON u.user_id = usc.user_id
JOIN 
	{{ ref('anal_user_click_count') }} ucc ON u.user_id = ucc.user_id
JOIN 
	{{ ref('anal_user_purchase_count') }} upc ON u.user_id = upc.user_id
GROUP BY 1