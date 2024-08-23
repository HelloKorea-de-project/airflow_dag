-----국가별 항공권 검색 수, 클릭 수, 구매 수
SELECT u.country, SUM(usc.search_count)as search_count, SUM(ucc.click_count) as click_count, SUM(upc.purchase_count) as purchase_count
FROM {{ ref('dim_user_info') }} u 
JOIN {{ ref('user_search_count') }} usc
	ON u.user_id = usc.user_id
JOIN {{ ref('user_click_count') }} ucc
	ON u.user_id = ucc.user_id
JOIN {{ ref('user_purchase_count') }} upc
	ON u.user_id = upc.user_id
GROUP BY country;