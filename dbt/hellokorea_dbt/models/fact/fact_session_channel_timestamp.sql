WITH src_session_channel_timestamp AS(
    SELECT * FROM {{ ref('src_session_channel_timestamp') }}
)
SELECT
    session_id,
    created_ts,
    last_connected_ts,
    channel
FROM
    src_session_channel_timestamp
WHERE
    created_ts IS NOT NULL
    {% if is_incremental() %}
        AND created_ts > (SELECT max(created_ts) FROM {{this}})
    {% endif %}