SELECT * 
FROM {{ source('raw_data', 'weather') }}
WHERE {{ filter_recent_update('created_at', 1) }}