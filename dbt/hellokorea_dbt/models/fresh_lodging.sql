SELECT * 
FROM {{ source('raw_data', 'lodging') }}
WHERE {{ filter_recent_update('created_at', 1) }}