WITH src_airline_search_log AS(
    SELECT * FROM {{ ref('src_airline_search_log') }}
)
SELECT
    user_id,
    searched_ts,
    depart_date,
    depart_airport_code,
    arrival_airport_code
FROM 
    src_airline_search_log
WHERE
    searched_ts IS NOT NULL
    {% if is_incremental() %}
        AND searched_ts > (SELECT max(searched_ts) from {{this}})
    {% endif %}