SELECT * 
FROM {{ source('raw_data', 'ex_rate') }}
WHERE {{ filter_recent_update('created_at', 1) }}
