WITH src_airline_click_log AS(
    SELECT * FROM raw_data.airline_click_log
)
SELECT
    user_id,
    session_id,
    clicked_ts,
    flight_id,
    depart_time,
    depart_airport_code,
    arrival_airport_code
FROM
    src_airline_click_log