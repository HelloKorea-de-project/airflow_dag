WITH src_session_channel_timestamp AS(
    SELECT * FROM {{ ref('src_session_channel_timestamp') }}
)
SELECT
    flight_id,
    pass_num,
    user_id,
    purchased_ts
FROM
    src_session_channel_timestamp
WHERE
    purchased_ts IS NOT NULL
    {% if is_incremental() %}
        AND purchased_ts > (SELECT max(purchased_ts) FROM {{this}})
    {% endif %}