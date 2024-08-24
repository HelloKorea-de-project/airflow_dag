SELECT 
	csc.channel, 
	csc.search_count AS search_count, 
	ccc.click_count AS click_count, 
	cpc.purchase_count AS purchase_count
FROM 
	{{ ref('anal_channel_search_count') }} csc
JOIN 
	{{ ref('anal_channel_click_count') }} ccc ON csc.channel = ccc.channel
JOIN 
	{{ ref('anal_channel_purchase_count') }} cpc ON csc.channel = cpc.channel