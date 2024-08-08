SELECT * 
FROM {{ source('raw_data', 'event_detail') }}
WHERE {{ filter_recent_update('updatedat', 1) }}