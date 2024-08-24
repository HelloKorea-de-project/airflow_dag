WITH src_airline_search_log AS(
    SELECT * FROM raw_data.airline_search_log
)
SELECT
    user_id,
    session_id,
    searched_ts,
    depart_date,
    depart_airport_code,
    arrival_airport_code
FROM 
    src_airline_search_log