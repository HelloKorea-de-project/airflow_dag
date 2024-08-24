SELECT 
    depart_airport_code, 
    COUNT(*) cnt_dep
FROM 
    {{ ref('fact_airline_click_log') }}
WHERE 
    clicked_ts >= DATEADD(day, -30, DATE '2024-08-04')
GROUP BY 1
ORDER BY 2 DESC