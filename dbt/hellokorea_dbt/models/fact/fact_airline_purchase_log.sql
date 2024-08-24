WITH src_airline_purchase_log AS(
    SELECT * FROM {{ ref('src_airline_purchase_log')}}
)
SELECT
    flight_id,
    purchased_ts,
    user_id,
    pass_num
FROM
    src_airline_purchase_log
WHERE purchased_ts IS NOT NULL
    {% if is_incremental() %}
        AND purchased_ts > (SELECT max(purchased_ts) from {{ this }})
    {% endif %}