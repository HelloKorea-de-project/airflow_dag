SELECT
    session_id,
    created_ts,
    last_connected_ts,
    channel
FROM
    {{ source('raw_data', 'session_channel_timestamp') }}