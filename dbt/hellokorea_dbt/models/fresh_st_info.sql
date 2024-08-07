SELECT * 
FROM {{ source('raw_data', 'st_info') }}
WHERE {{ filter_recent_update('updatedat', 1) }}
