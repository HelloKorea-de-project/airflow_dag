WITH src_session_channel_timestamp AS(
    SELECT * FROM raw_data.session_channel_timestamp
)
SELECT
    session_id,
    created_ts,
    last_connected_ts,
    channel
FROM
    src_session_channel_timestamp