SELECT * 
FROM {{ source('raw_data', 'serv_air_icn') }}
WHERE {{ filter_recent_update('extracteddate', 10) }}
