SELECT
    user_id,
    session_id,
    clicked_ts,
    flight_id,
    depart_time,
    depart_airport_code,
    arrival_airport_code
FROM
    {{ source('raw_data', 'airline_click_log') }}