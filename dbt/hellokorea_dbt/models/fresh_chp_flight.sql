SELECT * 
FROM {{ source('raw_data', 'chp_flight') }}
WHERE {{ filter_recent_update('extracteddate', 3) }}
