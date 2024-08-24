SELECT 
    apl.user_id, 
    asl.searched_ts, 
    apl.purchased_ts, 
    DATEDIFF(minute, asl.searched_ts, apl.purchased_ts) AS time_to_purchase
FROM 
    {{ ref('fact_airline_purchase_log') }} apl
JOIN 
    (SELECT 
        fasl.user_id, 
        min(fasl.searched_ts) as searched_ts 
    FROM 
        {{ ref('fact_airline_search_log') }} fasl 
    GROUP BY 1
    ) asl
ON apl.user_id = asl.user_id