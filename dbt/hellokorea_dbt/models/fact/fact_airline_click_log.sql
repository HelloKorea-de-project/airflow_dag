WITH src_airline_click_log AS(
    SELECT * FROM {{ ref('src_airline_click_log') }}
)
SELECT
    user_id,
    clicked_ts,
    flight_id,
    depart_time,
    depart_airport_code,
    arrival_airport_code
FROM
    src_airline_click_log
WHERE clicked_ts IS NOT NULL
    {% if is_incremental() %}
        AND clicked_ts > (SELECT max(clicked_ts) from {{this}})
    {% endif %}