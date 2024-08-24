SELECT
    flight_id,
    purchased_ts,
    user_id,
    pass_num
FROM 
    {{ source('raw_data', 'airline_purchase_log') }}