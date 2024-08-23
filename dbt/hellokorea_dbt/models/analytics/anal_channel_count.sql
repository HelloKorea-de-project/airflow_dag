SELECT csc.channel, SUM(csc.sum)as search_count, SUM(ccc.sum) as click_count, SUM(cpc.purchase_count) as purchase_count
FROM {{ ref('channel_search_count') }} csc
JOIN {{ ref('channel_click_count') }} ccc
	ON csc.channel = ccc.channel
JOIN {{ ref('channel_purchase_count' }} cpc
	ON csc.channel = cpc.channel
GROUP BY csc.channel;