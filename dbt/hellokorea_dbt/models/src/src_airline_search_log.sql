SELECT
    user_id,
    session_id,
    searched_ts,
    depart_date,
    depart_airport_code,
    arrival_airport_code
FROM 
    {{ source('raw_data', 'airline_search_log') }}