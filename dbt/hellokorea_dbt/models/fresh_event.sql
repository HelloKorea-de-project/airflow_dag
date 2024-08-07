SELECT * 
FROM {{ source('raw_data', 'event') }}
WHERE {{ filter_recent_update('updatedat', 1) }}
