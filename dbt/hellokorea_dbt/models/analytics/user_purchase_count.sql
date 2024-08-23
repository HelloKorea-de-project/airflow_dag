SELECT user_id, count(user_id) as purchase_count
        FROM {{ ref('fact_airline_purchase_log') }}
        WHERE user_id IS NOT NULL
        GROUP BY user_id