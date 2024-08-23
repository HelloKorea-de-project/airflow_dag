SELECT user_id, session_id, count(user_id) as search_count
        FROM {{ ref('fact_airline_search_log') }}
        WHERE user_id is not null
        GROUP BY user_id