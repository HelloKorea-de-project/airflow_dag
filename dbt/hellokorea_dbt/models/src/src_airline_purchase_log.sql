WITH src_airline_purchase_log AS(
    SELECT * FROM raw_data.airline_purchase_log
)
SELECT
    flight_id,
    purchased_ts,
    user_id,
    pass_num
FROM src_airline_purchase_log